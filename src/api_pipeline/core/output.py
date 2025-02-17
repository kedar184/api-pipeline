from abc import ABC, abstractmethod
from datetime import datetime, UTC
from typing import Dict, Any, List
import time
from pydantic import BaseModel
from loguru import logger


class OutputConfig(BaseModel):
    """Configuration for output handlers."""
    type: str                        # Type of output (e.g., "local_json", "bigquery", "gcs")
    enabled: bool = True             # Whether output is enabled
    config: Dict[str, Any]           # Output-specific configuration


class BaseOutput(ABC):
    """Base class for all output handlers with metrics and resource management."""

    def __init__(self, config: OutputConfig):
        self.config = config
        self._is_initialized: bool = False
        self._metrics: Dict[str, Any] = {
            'records_written': 0,
            'bytes_written': 0,
            'write_errors': 0,
            'last_write_time': None,
            'total_write_time': 0.0,
            'batch_count': 0,
            'failed_batches': 0
        }
        self._start_time = time.monotonic()

    async def __aenter__(self) -> 'BaseOutput':
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the output handler.
        
        This method should handle any setup required before writing data,
        such as creating tables or ensuring directories exist.
        
        Raises:
            Exception: If initialization fails
        """
        pass

    @abstractmethod
    async def write(self, data: List[Dict[str, Any]]) -> None:
        """Write data to the output destination.
        
        Args:
            data: List of records to write
            
        Raises:
            Exception: If write operation fails
        """
        start_time = time.monotonic()
        try:
            # Track metrics before actual write
            self._metrics['records_written'] += len(data)
            self._metrics['last_write_time'] = datetime.now(UTC)
            self._metrics['batch_count'] += 1
            
            # Subclasses should implement actual writing logic
            
        except Exception as e:
            self._metrics['write_errors'] += 1
            self._metrics['failed_batches'] += 1
            logger.error(f"Write operation failed: {str(e)}")
            raise
        finally:
            write_time = time.monotonic() - start_time
            self._metrics['total_write_time'] += write_time

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources.
        
        This method should handle proper cleanup of any resources,
        such as closing connections or flushing buffers.
        """
        pass

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics for monitoring."""
        current_time = time.monotonic()
        metrics = self._metrics.copy()
        
        # Calculate derived metrics
        uptime = current_time - self._start_time
        metrics['uptime'] = uptime
        
        metrics['records_per_second'] = (
            metrics['records_written'] / uptime
            if uptime > 0 else 0
        )
        
        metrics['average_write_time'] = (
            metrics['total_write_time'] / metrics['batch_count']
            if metrics['batch_count'] > 0 else 0
        )
        
        metrics['success_rate'] = (
            (metrics['batch_count'] - metrics['failed_batches']) / metrics['batch_count']
            if metrics['batch_count'] > 0 else 0
        )
        
        return metrics


class OutputError(Exception):
    """Base class for output-related errors."""
    pass


class InitializationError(OutputError):
    """Raised when output initialization fails."""
    pass


class WriteError(OutputError):
    """Raised when write operation fails."""
    pass


class CleanupError(OutputError):
    """Raised when cleanup fails."""
    pass 