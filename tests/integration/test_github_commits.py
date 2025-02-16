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
    OutputConfig
)
from api_pipeline.core.auth import AuthConfig
from pathlib import Path

def test_config():
    """Create a test configuration with optimized performance settings."""
    return ExtractorConfig(
        base_url="https://api.github.com",
        endpoints={
            "commits": "/repos/{repo}/commits"
        },
        auth_config=AuthConfig(
            auth_type="bearer",
            auth_credentials={"token": "placeholder"}  # Will be replaced with real token
        ),
        # Performance optimizations
        batch_size=50,  # Increased batch size for fewer API calls
        max_concurrent_requests=5,  # Reduced for better visualization of parallel execution
        session_timeout=30,
        
        # Pagination optimization
        pagination=PaginationConfig(
            enabled=True,
            strategy=PaginationType.PAGE_NUMBER,
            page_size=100,  # Maximum page size for GitHub API
            max_pages=10
        ),
        
        # Window configuration for efficient processing
        watermark=WatermarkConfig(
            enabled=True,
            timestamp_field="committed_at",  # Match the field in our transformed data
            window=WindowConfig(
                window_type=WindowType.FIXED,
                window_size="24h",  # 24-hour windows for 7-day period
                window_offset="0m",
                timestamp_field="committed_at"
            ),
            start_time_param="since",  # GitHub's parameter name
            end_time_param="until",    # GitHub's parameter name
            time_format="%Y-%m-%dT%H:%M:%SZ",  # GitHub's expected time format
            initial_watermark=datetime.now(UTC) - timedelta(days=7)  # Look back 7 days
        ),        
        # Retry configuration
        retry=RetryConfig(
            max_attempts=3,
            max_time=30,
            base_delay=1.0,
            max_delay=5.0
        )
    )

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
    config = test_config()
    
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
        commits = await extractor.extract(parameters={"repo": "apache/spark"})
        
        # Record end time
        overall_end_time = time.time()
        
        # Write commits to output
        await output_handler.write(commits)
        
        # Get metrics
        metrics = extractor.get_metrics()
        
        # Print detailed execution summary
        print("\nExecution Summary")
        print("================")
        print(f"Total Processing Time: {overall_end_time - overall_start_time:.2f} seconds")
        print(f"Items Processed: {metrics['items_processed']}")
        print(f"Windows Processed: {metrics['window_count']}")
        print(f"Average Time per Window: {metrics['total_processing_time'] / metrics['window_count']:.2f} seconds")
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
    test_config()  # Run the test first
    asyncio.run(main())