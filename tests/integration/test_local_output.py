import os
import asyncio
from datetime import datetime, UTC
from api_pipeline.core.base import OutputConfig
from api_pipeline.outputs.local_json import LocalJsonOutput

def serialize_datetime(dt: datetime) -> str:
    """Convert datetime to ISO format string."""
    return dt.isoformat()

async def main():
    # Create sample data with serialized datetimes
    current_time = datetime.now(UTC)
    sample_data = [
        {
            "commit_id": "abc123",
            "repo_name": "test-repo",
            "author_name": "Test Author",
            "author_email": "test@example.com",
            "message": "Test commit",
            "commit_url": "https://github.com/test/repo/commit/abc123",
            "committed_at": serialize_datetime(current_time),
            "files_changed": 1,
            "additions": 10,
            "deletions": 5,
            "processed_at": serialize_datetime(current_time)
        }
    ]

    # Create output config
    output_config = OutputConfig(
        type="local_json",
        enabled=True,
        config={
            "output_dir": "./data/github/commits",
            "file_format": "jsonl"
        }
    )

    # Create and initialize the output handler
    output_handler = LocalJsonOutput(output_config)
    
    try:
        # Write sample data
        await output_handler.write(sample_data)
        print(f"Successfully wrote {len(sample_data)} records")
    finally:
        # Close the output handler
        await output_handler.close()

if __name__ == "__main__":
    asyncio.run(main())