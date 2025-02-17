#!/usr/bin/env python3
import os
import asyncio
import time
from datetime import datetime, UTC, timedelta
from dotenv import load_dotenv
from api_pipeline.extractors.github_commits import GitHubCommitsExtractor
from api_pipeline.outputs.local_json import LocalJsonOutput
from api_pipeline.core.base import (
    ExtractorConfig,
    PaginationConfig,
    RetryConfig,
    WatermarkConfig,
    WindowConfig,
    WindowType,
    PaginationType,
    OutputConfig,
    ParallelProcessingStrategy,
    TimeWindowParallelConfig,
    ProcessingPattern
)
from api_pipeline.core.auth import AuthConfig
from pathlib import Path
import pytest

@pytest.mark.asyncio
async def test_config():
    """Test GitHub commits extraction with parallel window processing."""
    # Load environment variables from .env file
    dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
    load_dotenv(dotenv_path)
    
    # Get the GitHub token and verify it's set
    github_token = os.getenv('LOCAL_GITHUB_TOKEN')
    if not github_token:
        raise ValueError("LOCAL_GITHUB_TOKEN environment variable is not set. Please check your .env file.")

    # Create base config with parallel window processing
    config = ExtractorConfig(
        base_url="https://api.github.com",
        endpoints={
            "current": "/repos/{repo}/commits"
        },
        auth_config=AuthConfig(
            auth_type="bearer",
            auth_credentials={"token": github_token}
        ),
        # Use SINGLE processing pattern
        processing_pattern=ProcessingPattern.SINGLE,
        
        # Optimize for parallel processing
        batch_size=20,  # Smaller batch size to see multiple batches
        max_concurrent_requests=5,  # Allow multiple concurrent requests
        session_timeout=30,
        
        # Configure pagination with parallel processing
        pagination=PaginationConfig.with_link_header(
            page_size=30,  # Smaller page size to see multiple pages
            parse_params=True,
            rel_next="next"
        ),
        
        # Enable watermark with multiple windows
        watermark=WatermarkConfig(
            enabled=True,
            timestamp_field="committed_at",
            initial_watermark=datetime.now(UTC) - timedelta(days=1),  # Look back 1 day
            window=WindowConfig(
                window_type=WindowType.FIXED,
                window_size="12h",  # 12-hour windows
                window_overlap="2h",  # 2-hour overlap
                timestamp_field="committed_at"
            ),
            time_format="%Y-%m-%dT%H:%M:%SZ",
            start_time_param="since",
            end_time_param="until"
        ),
        
        # Configure retry with shorter delays for testing
        retry=RetryConfig(
            max_attempts=3,
            max_time=30,
            base_delay=1.0,
            max_delay=5.0
        )
    )

    # Test single repo extraction
    print("\nTesting commit extraction...")
    await extract_and_validate(config)
    
    return config

async def extract_and_validate(config: ExtractorConfig):
    """Helper function to test extraction with a given configuration."""
    # Create output config optimized for parallel writes
    output_config = OutputConfig(
        type="local_json",
        enabled=True,
        config={
            "output_dir": "./data/github/commits/single",
            "file_format": "jsonl",
            "partition_by": ["date", "repo"],
            "batch_size": 10,  # Small batch size for testing parallel writes
            "max_concurrent_writes": 5  # Allow multiple concurrent writes
        }
    )

    # Create and initialize the extractor and output handler
    extractor = GitHubCommitsExtractor(config)
    output_handler = LocalJsonOutput(output_config)
    
    try:
        # Record start time
        overall_start_time = time.time()
        
        # Extract and write commits using streaming
        commit_count = 0
        first_commit = None
        
        async for commit in extractor.extract_stream(parameters={"repo": "pallets/flask"}):
            if first_commit is None:
                first_commit = commit
            commit_count += 1
            
            # Write commits in real-time
            await output_handler.write([commit])
        
        # Record end time
        overall_end_time = time.time()
        
        # Get metrics from all components
        extractor_metrics = extractor.get_metrics()
        output_metrics = output_handler.get_metrics()
        
        print("\nExtraction Results")
        print("=================")
        print(f"Total Processing Time: {overall_end_time - overall_start_time:.2f} seconds")
        print(f"Items Processed: {extractor_metrics['items_processed']}")
        print(f"Windows Processed: {extractor_metrics['window_count']}")
        print(f"Number of Commits: {commit_count}")
        
        if first_commit:
            print(f"First Commit SHA: {first_commit['sha']}")
            print(f"First Commit Date: {first_commit['committed_at']}")
            print(f"First Commit Author: {first_commit['commit']['author']['name']}")
        
    finally:
        # Clean up
        await output_handler.close()

async def main():
    """Run the GitHub commits extraction with performance tracking."""
    # Load environment variables from .env file
    dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
    load_dotenv(dotenv_path)
    
    # Get the GitHub token and verify it's set
    github_token = os.getenv('LOCAL_GITHUB_TOKEN')
    if not github_token:
        raise ValueError("LOCAL_GITHUB_TOKEN environment variable is not set. Please check your .env file.")

    # Create auth config with environment token
    auth_config = AuthConfig(
        auth_type="bearer",
        auth_credentials={
            "token": github_token
        }
    )

    # Create output config
    output_config = OutputConfig(
        type="local_json",
        enabled=True,
        config={
            "output_dir": "./data/github/commits",
            "file_format": "jsonl",
            "partition_by": ["date", "repo"],
            "batch_size": 20,  # Small batch size for testing
            "max_concurrent_writes": 5  # Allow up to 5 concurrent writes
        }
    )

    # Get the base config
    config = await test_config()
    
    # Update auth config with real token
    config.auth_config = auth_config

    # Create and initialize the extractor
    extractor = GitHubCommitsExtractor(config)
    
    # Create and initialize the output handler
    output_handler = LocalJsonOutput(output_config)
    
    try:
        # Record start time
        overall_start_time = time.time()
        
        # Extract commits
        commits = await extractor.extract(parameters={"repo": "pallets/flask"})
        
        # Record end time
        overall_end_time = time.time()
        
        # Write commits to output
        await output_handler.write(commits)
        
        # Get metrics from all components
        extractor_metrics = extractor.get_metrics()
        output_metrics = output_handler.get_metrics()
        auth_metrics = extractor.auth_handler.get_metrics()
        retry_metrics = extractor.retry_handler.get_metrics()
        if hasattr(extractor, 'watermark_handler'):
            watermark_metrics = extractor.watermark_handler.get_metrics()
        
        # Print detailed metrics summary
        print("\nDetailed Metrics Summary")
        print("======================")
        
        print("\nExtractor Metrics")
        print("----------------")
        for key, value in sorted(extractor_metrics.items()):
            if isinstance(value, (int, float)):
                print(f"{key}: {value:.2f}" if isinstance(value, float) else f"{key}: {value}")
            else:
                print(f"{key}: {value}")
        
        print("\nOutput Metrics")
        print("-------------")
        for key, value in sorted(output_metrics.items()):
            if isinstance(value, (int, float)):
                print(f"{key}: {value:.2f}" if isinstance(value, float) else f"{key}: {value}")
            else:
                print(f"{key}: {value}")
        
        print("\nAuth Metrics")
        print("------------")
        for key, value in sorted(auth_metrics.items()):
            if isinstance(value, (int, float)):
                print(f"{key}: {value:.2f}" if isinstance(value, float) else f"{key}: {value}")
            else:
                print(f"{key}: {value}")
        
        print("\nRetry Metrics")
        print("-------------")
        for key, value in sorted(retry_metrics.items()):
            if isinstance(value, (int, float)):
                print(f"{key}: {value:.2f}" if isinstance(value, float) else f"{key}: {value}")
            else:
                print(f"{key}: {value}")
        
        if hasattr(extractor, 'watermark_handler'):
            print("\nWatermark Metrics")
            print("----------------")
            for key, value in sorted(watermark_metrics.items()):
                if isinstance(value, (int, float)):
                    print(f"{key}: {value:.2f}" if isinstance(value, float) else f"{key}: {value}")
                else:
                    print(f"{key}: {value}")
        
        print("\nExecution Summary")
        print("================")
        print(f"Total Processing Time: {overall_end_time - overall_start_time:.2f} seconds")
        print(f"Items Processed: {extractor_metrics['items_processed']}")
        print(f"Windows Processed: {extractor_metrics['window_count']}")
        if extractor_metrics['window_count'] > 0:
            print(f"Average Time per Window: {extractor_metrics['total_processing_time'] / extractor_metrics['window_count']:.2f} seconds")
        print(f"Concurrent Requests: {config.max_concurrent_requests}")
        print(f"Batch Size: {config.batch_size}")
        
        if commits:
            # Analyze timestamp distribution
            timestamps = [commit['committed_at'] for commit in commits]
            timestamps.sort()
            
            print("\nTimestamp Analysis")
            print("==================")
            print(f"Earliest Commit: {timestamps[0]}")
            print(f"Latest Commit: {timestamps[-1]}")
            print(f"Total Time Range: {timestamps[-1] - timestamps[0]}")
            print(f"Number of Commits: {len(commits)}")
            
            # Analyze commits per day
            from collections import defaultdict
            commits_per_day = defaultdict(int)
            for ts in timestamps:
                commits_per_day[ts.date()] += 1
            
            print("\nCommits per Day")
            print("===============")
            for date, count in sorted(commits_per_day.items()):
                print(f"{date}: {count} commits")
        
        # Print output file info
        print(f"\nCommits have been written to {output_config.config['output_dir']}")
        
    finally:
        # Clean up
        await output_handler.close()

if __name__ == "__main__":
    asyncio.run(main())  # Run main directly when script is executed