#!/usr/bin/env python3
import os
import asyncio
import time
from datetime import datetime, UTC
from dotenv import load_dotenv
from api_pipeline.extractors.github_users import GitHubUsersExtractor
from api_pipeline.outputs.local_json import LocalJsonOutput
from api_pipeline.core.base import (
    ExtractorConfig,
    RetryConfig,
    OutputConfig,
    ProcessingPattern
)
from api_pipeline.core.auth import AuthConfig

def test_batch_processing():
    """Test GitHub users extraction with BATCH processing pattern.
    
    This test demonstrates:
    1. BATCH processing pattern where each user requires a separate API call
    2. Parallel processing of user batches
    3. Rate limiting and retry handling
    4. Metrics tracking for both extraction and output
    """
    # Load environment variables from .env file
    dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
    load_dotenv(dotenv_path)
    
    # Get the GitHub token and verify it's set
    github_token = os.getenv('LOCAL_GITHUB_TOKEN')
    if not github_token:
        raise ValueError("LOCAL_GITHUB_TOKEN environment variable is not set. Please check your .env file.")

    # Create extractor config optimized for batch processing
    config = ExtractorConfig(
        base_url="https://api.github.com",
        endpoints={
            "current": "/users/{username}"
        },
        auth_config=AuthConfig(
            auth_type="bearer",
            auth_credentials={"token": github_token}
        ),
        # Set BATCH processing pattern as each user needs a separate request
        processing_pattern=ProcessingPattern.BATCH,
        batch_parameter_name="usernames",
        
        # Optimize for parallel processing
        batch_size=20,  # Process 20 users at a time
        max_concurrent_requests=5,  # Allow 5 concurrent API calls
        session_timeout=30,
        
        # Configure retry with shorter delays for testing
        retry=RetryConfig(
            max_attempts=3,
            max_time=30,
            base_delay=1.0,
            max_delay=5.0
        )
    )

    # Create output config
    output_config = OutputConfig(
        type="local_json",
        enabled=True,
        config={
            "output_dir": "./data/github/users",
            "file_format": "jsonl",
            "partition_by": ["type"],  # Partition by user type (User/Organization)
            "batch_size": 10,  # Write in small batches
            "max_concurrent_writes": 5  # Allow multiple concurrent writes
        }
    )

    # List of interesting GitHub users to fetch
    # Mix of individual users and organizations
    usernames = [
        # Notable individuals
        "torvalds",      # Linus Torvalds
        "gvanrossum",    # Guido van Rossum
        "dhh",           # David Heinemeier Hansson
        "jeresig",       # John Resig
        "tj",            # TJ Holowaychuk
        
        # Organizations
        "google",
        "microsoft",
        "facebook",
        "apple",
        "netflix",
        
        # More individuals
        "mrdoob",        # Ricardo Cabello
        "addyosmani",    # Addy Osmani
        "paulirish",     # Paul Irish
        "sindresorhus",  # Sindre Sorhus
        "defunkt",       # Chris Wanstrath
        
        # More organizations
        "apache",
        "mozilla",
        "kubernetes",
        "tensorflow",
        "aws"
    ]

    # Create and initialize the extractor and output handler
    extractor = GitHubUsersExtractor(config)
    output_handler = LocalJsonOutput(output_config)
    
    try:
        # Record start time
        overall_start_time = time.time()
        
        # Extract user details with batch processing
        users = asyncio.run(extractor.extract(parameters={"usernames": usernames}))
        
        # Record end time
        overall_end_time = time.time()
        
        # Write users to output with parallel processing
        asyncio.run(output_handler.write(users))
        
        # Get metrics from all components
        extractor_metrics = extractor.get_metrics()
        output_metrics = output_handler.get_metrics()
        auth_metrics = extractor.auth_handler.get_metrics()
        retry_metrics = extractor.retry_handler.get_metrics()
        
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
        
        print("\nBatch Processing Summary")
        print("=======================")
        print(f"Total Users: {len(usernames)}")
        print(f"Batch Size: {config.batch_size}")
        print(f"Number of Batches: {extractor_metrics['batch_count']}")
        print(f"Concurrent Requests: {config.max_concurrent_requests}")
        print(f"Failed Batches: {extractor_metrics['failed_batches']}")
        print(f"Success Rate: {extractor_metrics['success_rate']:.2%}")
        
        print("\nExecution Summary")
        print("================")
        print(f"Total Processing Time: {overall_end_time - overall_start_time:.2f} seconds")
        print(f"Items Processed: {extractor_metrics['items_processed']}")
        print(f"Average Time per Item: {extractor_metrics['average_processing_time_per_item']:.2f} seconds")
        
        if users:
            # Analyze user types
            user_types = {}
            for user in users:
                user_type = user['type']
                user_types[user_type] = user_types.get(user_type, 0) + 1
            
            print("\nUser Type Analysis")
            print("=================")
            for user_type, count in sorted(user_types.items()):
                print(f"{user_type}: {count} users")
        
        print(f"\nUser details have been written to {output_config.config['output_dir']}")
        
    finally:
        # Clean up
        asyncio.run(output_handler.close())
        
    return config

if __name__ == "__main__":
    asyncio.run(test_batch_processing())  # Run test directly when script is executed 