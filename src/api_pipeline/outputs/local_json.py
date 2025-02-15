import os
import json
from datetime import datetime, UTC
from typing import Any, Dict, List
from pathlib import Path
from loguru import logger

from api_pipeline.core.base import BaseOutput, OutputConfig

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class LocalJsonOutput(BaseOutput):
    """Handler for writing data to local JSON files."""

    def __init__(self, config: OutputConfig):
        super().__init__(config)
        self.output_dir = Path(self.config.config["output_dir"])
        self.file_format = self.config.config.get("file_format", "jsonl")
        self.partition_by = self.config.config.get("partition_by", None)
        self.current_file = None
        self._ensure_output_dir()

    async def initialize(self) -> None:
        """Initialize the output handler."""
        self._ensure_output_dir()
        self._is_initialized = True

    def _ensure_output_dir(self) -> None:
        """Ensure the output directory exists."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured output directory exists: {self.output_dir}")

    def _get_partition_path(self, data: Dict[str, Any]) -> str:
        """Get the partition path based on configuration."""
        if not self.partition_by:
            return ""

        if isinstance(self.partition_by, str):
            # Single partition field
            if self.partition_by == "date":
                return datetime.now(UTC).strftime("%Y/%m/%d")
            return str(data.get(self.partition_by, "unknown"))
        
        # Multiple partition fields
        return "/".join(str(data.get(field, "unknown")) 
                       for field in self.partition_by)

    def _get_output_file(self, partition_path: str) -> Path:
        """Get the output file path with timestamp."""
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        filename = f"data_{timestamp}.{self.file_format}"
        
        if partition_path:
            full_path = self.output_dir / partition_path
            full_path.mkdir(parents=True, exist_ok=True)
            return full_path / filename
        
        return self.output_dir / filename

    async def write(self, data: List[Dict[str, Any]]) -> None:
        """Write data to a local JSON file."""
        if not data:
            logger.warning("No data to write")
            return

        try:
            # Get partition path from first record
            partition_path = self._get_partition_path(data[0])
            output_file = self._get_output_file(partition_path)
            
            # Write data based on format
            if self.file_format == "jsonl":
                with open(output_file, "w") as f:
                    for record in data:
                        f.write(json.dumps(record, cls=DateTimeEncoder) + "\n")
            else:  # regular json
                with open(output_file, "w") as f:
                    json.dump(data, f, indent=2, cls=DateTimeEncoder)
            
            logger.info(f"Successfully wrote {len(data)} records to {output_file}")
            
        except Exception as e:
            logger.error(f"Error writing to local JSON file: {str(e)}")
            raise

    async def close(self) -> None:
        """Clean up resources."""
        pass  # No cleanup needed for file operations 