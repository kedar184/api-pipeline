from datetime import datetime, timedelta, UTC
from typing import List, Tuple, Optional, Dict, Any
from pydantic import BaseModel
from loguru import logger

from api_pipeline.core.types import ParallelProcessingStrategy
from api_pipeline.core.utils import parse_datetime


class TimeWindowParallelConfig(BaseModel):
    """Configuration for time-based parallel processing."""
    window_size: str = "24h"        # Duration of each window (e.g., "24h", "7d", "1h")
    window_overlap: str = "0m"      # Overlap between windows (e.g., "1h" for 1 hour overlap)
    max_concurrent_windows: int = 5  # Maximum number of windows to process in parallel
    timestamp_format: str = "%Y-%m-%dT%H:%M:%SZ"  # Format for time parameters in API requests

    @property
    def window_size_seconds(self) -> int:
        """Convert window size string to seconds."""
        return self._parse_duration(self.window_size)
    
    @property
    def window_overlap_seconds(self) -> int:
        """Convert window overlap string to seconds."""
        return self._parse_duration(self.window_overlap)
    
    def _parse_duration(self, duration: str) -> int:
        """Parse duration string (e.g., "1h", "30m") to seconds."""
        unit = duration[-1].lower()
        value = int(duration[:-1])
        
        if unit == 's':
            return value
        elif unit == 'm':
            return value * 60
        elif unit == 'h':
            return value * 3600
        elif unit == 'd':
            return value * 86400
        else:
            raise ValueError(f"Unsupported duration unit: {unit}")


class KnownPagesParallelConfig(BaseModel):
    """Configuration for parallel processing with known total pages."""
    max_concurrent_pages: int = 5    # Maximum number of pages to fetch in parallel
    require_total_count: bool = True # If True, expects API to return total count (e.g., {"total": 100}). If False, discovers total by paging until no more results
    safe_page_size: int = 100       # Number of items per page (e.g., 100 for GitHub API)


class CalculatedOffsetParallelConfig(BaseModel):
    """Configuration for parallel processing with calculated offsets."""
    max_concurrent_chunks: int = 5   # Maximum number of chunks to process in parallel
    chunk_size: int = 1000          # Number of items in each chunk (e.g., 1000 for large datasets)
    require_total_count: bool = True # If True, uses total from API for optimal chunk calculation. If False, dynamically adjusts chunks as data is discovered


# Type alias for all parallel configuration types
ParallelConfig = TimeWindowParallelConfig | KnownPagesParallelConfig | CalculatedOffsetParallelConfig


def calculate_time_windows(
    start_time: datetime,
    end_time: datetime,
    config: TimeWindowParallelConfig
) -> List[Tuple[datetime, datetime]]:
    """Calculate time windows for parallel processing.
    
    Args:
        start_time: Start time for windowing
        end_time: End time for windowing
        config: Time window parallel configuration
        
    Returns:
        List of (window_start, window_end) tuples
    """
    logger.info(f"Calculating time windows from {start_time.isoformat()} to {end_time.isoformat()}")
    logger.info(f"Window size: {config.window_size}, overlap: {config.window_overlap}")
    
    windows = []
    current_start = start_time
    
    while current_start < end_time:
        window_end = min(
            current_start + timedelta(seconds=config.window_size_seconds),
            end_time
        )
        windows.append((current_start, window_end))
        current_start = window_end - timedelta(seconds=config.window_overlap_seconds)
    
    logger.info(f"Generated {len(windows)} windows")
    for i, (w_start, w_end) in enumerate(windows):
        logger.info(f"Window {i+1}: {w_start.isoformat()} to {w_end.isoformat()}")
    
    return windows


def calculate_offset_chunks(
    total_items: int,
    config: CalculatedOffsetParallelConfig
) -> List[Tuple[int, int]]:
    """Calculate offset chunks for parallel processing.
    
    Args:
        total_items: Total number of items to process
        config: Offset parallel configuration
        
    Returns:
        List of (start_offset, end_offset) tuples
    """
    logger.info(f"Calculating offset chunks for {total_items} items")
    logger.info(f"Chunk size: {config.chunk_size}")
    
    chunks = []
    for offset in range(0, total_items, config.chunk_size):
        end_offset = min(offset + config.chunk_size, total_items)
        chunks.append((offset, end_offset))
    
    logger.info(f"Generated {len(chunks)} chunks")
    for i, (start, end) in enumerate(chunks):
        logger.info(f"Chunk {i+1}: items {start} to {end}")
    
    return chunks


def get_time_range(
    parameters: Dict[str, Any],
    start_param: str,
    end_param: str,
    default_lookback: int = 7
) -> Tuple[datetime, datetime]:
    """Get time range from parameters or use defaults.
    
    Args:
        parameters: Request parameters
        start_param: Parameter name for start time
        end_param: Parameter name for end time
        default_lookback: Default number of days to look back
        
    Returns:
        Tuple of (start_time, end_time)
    """
    end_time = (
        parse_datetime(parameters[end_param])
        if end_param in parameters
        else datetime.now(UTC)
    )
    
    start_time = (
        parse_datetime(parameters[start_param])
        if start_param in parameters
        else end_time - timedelta(days=default_lookback)
    )
    
    return start_time, end_time 