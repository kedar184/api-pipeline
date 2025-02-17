import os
import json
import asyncio
from datetime import datetime, UTC
from typing import Any, Dict, List, AsyncIterator
from pathlib import Path
from loguru import logger
import time
import aiofiles

from api_pipeline.core.base import BaseOutput, OutputConfig

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class LocalJsonOutput(BaseOutput):
    """Handler for writing data to local JSON files with parallel batch processing."""

    def __init__(self, config: OutputConfig):
        super().__init__(config)
        self.output_dir = Path(self.config.config["output_dir"])
        self.file_format = self.config.config.get("file_format", "jsonl")
        self.partition_by = self.config.config.get("partition_by", None)
        self.batch_size = self.config.config.get("batch_size", 1000)  # Default batch size
        self.max_concurrent_writes = self.config.config.get("max_concurrent_writes", 5)  # Limit concurrent writes
        self._semaphore = asyncio.Semaphore(self.max_concurrent_writes)
        self._metrics = {
            'records_written': 0,
            'bytes_written': 0,
            'write_errors': 0,
            'total_write_time': 0.0,
            'last_write_time': None,
            'batch_count': 0,
            'failed_batches': 0,
            'total_batch_size': 0,
            'max_batch_size': 0,
            'min_batch_size': float('inf'),
            'average_batch_size': 0,
            'bytes_per_second': 0.0,
            'records_per_second': 0.0,
            'error_rate': 0.0,
            'success_rate': 0.0
        }
        self._start_time = time.monotonic()
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

    async def _write_batch(self, batch: List[Dict[str, Any]], partition_path: str) -> None:
        """Write a batch of records to a partition."""
        if not batch:
            return

        try:
            # Get the output file path for this partition
            output_file = self._get_output_file(partition_path)
            
            # Write the batch to the file
            await self._write_to_file(batch, output_file)
            
            # Update metrics
            self._metrics['batch_count'] += 1
            self._metrics['total_batch_size'] += len(batch)
            self._metrics['max_batch_size'] = max(self._metrics['max_batch_size'], len(batch))
            self._metrics['min_batch_size'] = min(self._metrics['min_batch_size'], len(batch))
            
        except Exception as e:
            logger.error(f"Error writing batch to {partition_path}: {e}")
            self._metrics['failed_batches'] += 1
            raise

    async def write(self, data: List[Dict[str, Any]]) -> None:
        """Write data to local JSON files in parallel batches."""
        if not data:
            logger.warning("No data to write")
            return

        try:
            write_start_time = time.monotonic()
            
            # Group data by partition path
            partitioned_data: Dict[str, List[Dict[str, Any]]] = {}
            for record in data:
                partition_path = self._get_partition_path(record)
                if partition_path not in partitioned_data:
                    partitioned_data[partition_path] = []
                partitioned_data[partition_path].append(record)

            # Process each partition's data in batches
            write_tasks = []
            for partition_path, partition_data in partitioned_data.items():
                # Split partition data into batches
                for i in range(0, len(partition_data), self.batch_size):
                    batch = partition_data[i:i + self.batch_size]
                    write_tasks.append(self._write_batch(batch, partition_path))

            # Write batches in parallel
            await asyncio.gather(*write_tasks)
            
            # Update metrics
            self._metrics['records_written'] += len(data)
            self._metrics['last_write_time'] = datetime.now(UTC)
            
            # Calculate derived metrics
            uptime = time.monotonic() - self._start_time
            if uptime > 0:
                self._metrics['bytes_per_second'] = self._metrics['bytes_written'] / uptime
                self._metrics['records_per_second'] = self._metrics['records_written'] / uptime
            
            if self._metrics['batch_count'] > 0:
                self._metrics['average_batch_size'] = self._metrics['total_batch_size'] / self._metrics['batch_count']
                self._metrics['error_rate'] = self._metrics['failed_batches'] / self._metrics['batch_count']
                self._metrics['success_rate'] = 1 - self._metrics['error_rate']
            
        except Exception as e:
            logger.error(f"Error writing to local JSON file: {str(e)}")
            self._metrics['write_errors'] += 1
            raise

    async def close(self) -> None:
        """Clean up resources."""
        pass  # No cleanup needed for file operations

    async def write_stream(self, data_stream: AsyncIterator[Dict[str, Any]]) -> None:
        """Write data from a stream to the output destination."""
        batch = []
        batch_size = self.config.config.get('batch_size', 100)
        
        async for item in data_stream:
            batch.append(item)
            if len(batch) >= batch_size:
                await self._write_batch(batch)
                batch = []
        
        # Write any remaining items
        if batch:
            await self._write_batch(batch)

    async def _write_to_file(self, batch: List[Dict[str, Any]], output_file: Path) -> None:
        """Write a batch of items to a file."""
        try:
            # Update batch metrics
            batch_size = len(batch)
            
            # Write data based on format
            if self.file_format == "jsonl":
                async with aiofiles.open(output_file, "w") as f:
                    for record in batch:
                        line = json.dumps(record, cls=DateTimeEncoder) + "\n"
                        await f.write(line)
                        self._metrics['bytes_written'] += len(line.encode())
            else:  # regular json
                async with aiofiles.open(output_file, "w") as f:
                    json_data = json.dumps(batch, indent=2, cls=DateTimeEncoder)
                    await f.write(json_data)
                    self._metrics['bytes_written'] += len(json_data.encode())
            
            # Update timing metrics
            self._metrics['last_write_time'] = datetime.now(UTC)
            logger.info(f"Successfully wrote batch of {batch_size} records to {output_file}")
            
        except Exception as e:
            logger.error(f"Error writing to file {output_file}: {e}")
            self._metrics['write_errors'] += 1
            self._metrics['failed_batches'] += 1
            raise

    async def _write_to_partition(self, item: Dict[str, Any], partition_path: str) -> None:
        """Write an item to a partition."""
        try:
            # Update batch metrics
            batch_size = 1
            self._metrics['batch_count'] += 1
            self._metrics['total_batch_size'] += batch_size
            self._metrics['max_batch_size'] = max(self._metrics['max_batch_size'], batch_size)
            self._metrics['min_batch_size'] = min(self._metrics['min_batch_size'], batch_size)
            
            # Update record metrics
            self._metrics['records_written'] += 1
            self._metrics['last_write_time'] = datetime.now(UTC)
            
            # Write the item
            output_file = self._get_output_file(partition_path)
            with open(output_file, "a") as f:
                line = json.dumps(item, cls=DateTimeEncoder) + "\n"
                f.write(line)
                self._metrics['bytes_written'] += len(line.encode())
            
        except Exception as e:
            self._metrics['write_errors'] += 1
            self._metrics['failed_batches'] += 1
            raise

    async def _write_to_partition(self, item: Dict[str, Any], partition_by: List[str]) -> None:
        """Write an item to a partition."""
        try:
            # Update batch metrics
            batch_size = 1
            self._metrics['batch_count'] += 1
            self._metrics['total_batch_size'] += batch_size
            self._metrics['max_batch_size'] = max(self._metrics['max_batch_size'], batch_size)
            self._metrics['min_batch_size'] = min(self._metrics['min_batch_size'], batch_size)
            
            # Update record metrics
            self._metrics['records_written'] += 1
            self._metrics['last_write_time'] = datetime.now(UTC)
            
            # Write the item
            partition_path = self._get_partition_path(item)
            output_file = self._get_output_file(partition_path)
            with open(output_file, "a") as f:
                line = json.dumps(item, cls=DateTimeEncoder) + "\n"
                f.write(line)
                self._metrics['bytes_written'] += len(line.encode())
            
        except Exception as e:
            self._metrics['write_errors'] += 1
            self._metrics['failed_batches'] += 1
            raise 