import os
import asyncio
import time
from datetime import datetime, UTC
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
            "commits": "/repos/{owner}/{repo}/commits"
        },
        auth_config=AuthConfig(
            auth_type="bearer",
            auth_credentials={"token": "placeholder"}  # Will be replaced with real token
        ),
        # Performance optimizations
        batch_size=50,  # Increased batch size for fewer API calls
        max_concurrent_requests=10,  # Increased concurrent requests
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
            timestamp_field="committed_at",
            window=WindowConfig(
                window_type=WindowType.FIXED,
                window_size="24h",  # Larger window size for fewer iterations
                timestamp_field="committed_at"
            ),
            lookback_window="7d"  # One week of history
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
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
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
            "partition_by": ["date", "repo"]
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
        # Extract commits
        commits = await extractor.extract()
        
        # Write commits to output
        await output_handler.write(commits)
        
        # Get metrics
        metrics = extractor.get_metrics()
        
        # Print minimal execution summary
        print("\nExecution Summary")
        print("================")
        print(f"Total Processing Time: {metrics['total_processing_time']:.2f} seconds")
        print(f"Items Processed: {metrics['items_processed']}")
        
        # Print output file info
        print(f"\nCommits have been written to {output_config.config['output_dir']}")
        
    finally:
        # Clean up
        await output_handler.close()

if __name__ == "__main__":
    test_config()  # Run the test first
    asyncio.run(main())