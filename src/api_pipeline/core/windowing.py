from datetime import datetime, timedelta, UTC
from typing import List, Tuple, Optional
from pydantic import BaseModel
from loguru import logger

from api_pipeline.core.types import WindowType


class WindowConfig(BaseModel):
    # Configuration for time-based windowing with different window types and sizes
    window_type: WindowType = WindowType.FIXED  # Type of window (fixed, sliding, session)
    window_size: str = "1h"                     # Duration string (e.g., "1h", "1d", "7d")
    window_offset: str = "0m"                   # Offset for window start
    window_overlap: str = "0m"                  # For sliding windows
    timestamp_field: str = "timestamp"          # Field to use for windowing
    
    @property
    def window_size_seconds(self) -> int:
        # Convert window size string to seconds
        return self._parse_duration(self.window_size)
    
    @property
    def window_offset_seconds(self) -> int:
        # Convert window offset string to seconds
        return self._parse_duration(self.window_offset)
    
    @property
    def window_overlap_seconds(self) -> int:
        # Convert window overlap string to seconds
        return self._parse_duration(self.window_overlap)
    
    @staticmethod
    def _parse_duration(duration: str) -> int:
        # Parse duration string (e.g., "1h", "30m") to seconds
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


class WatermarkConfig(BaseModel):
    # Configuration for watermark-based incremental extraction
    enabled: bool = False                                  # Whether watermarking is enabled
    timestamp_field: str = "updated_at"                   # Field to track for watermark
    watermark_field: str = "last_watermark"              # Field to store watermark value
    window: Optional[WindowConfig] = None                 # Window configuration
    initial_watermark: Optional[datetime] = None          # Starting watermark value
    lookback_window: str = "0m"                          # How far to look back from watermark
    
    # Time range parameter names for API requests
    start_time_param: str = "start_time"                 # Parameter name for start time
    end_time_param: str = "end_time"                     # Parameter name for end time
    time_format: str = "%Y-%m-%dT%H:%M:%SZ"             # Format for time parameters
    
    @property
    def lookback_seconds(self) -> int:
        # Convert lookback window string to seconds
        if not self.lookback_window:
            return 0
        return WindowConfig._parse_duration(self.lookback_window)


class TimeWindowParallelConfig(BaseModel):
    window_size: str = "24h"        # Duration of each window (e.g., "24h", "7d", "1h")
    window_overlap: str = "0m"      # Overlap between windows (e.g., "1h" for 1 hour overlap)
    max_concurrent_windows: int = 5  # Maximum number of windows to process in parallel
    timestamp_format: str = "%Y-%m-%dT%H:%M:%SZ"  # Format for time parameters in API requests


def calculate_window_bounds(
    start_time: datetime,
    end_time: datetime,
    window_config: WindowConfig
) -> List[Tuple[datetime, datetime]]:
    """Calculate window bounds for the extraction period.
    
    Args:
        start_time: Start time for windowing
        end_time: End time for windowing
        window_config: Window configuration
        
    Returns:
        List of (window_start, window_end) tuples
    """
    if not window_config:
        logger.warning("No window configuration, using single window")
        return [(start_time, end_time)]
        
    window_size = window_config.window_size_seconds
    window_overlap = window_config.window_overlap_seconds
    
    logger.info(f"Calculating windows from {start_time.isoformat()} to {end_time.isoformat()}")
    logger.info(f"Window size: {window_size} seconds")
    
    windows = []
    window_start = start_time
    
    while window_start < end_time:
        window_end = min(
            window_start + timedelta(seconds=window_size),
            end_time
        )
        windows.append((window_start, window_end))
        window_start = window_end - timedelta(seconds=window_overlap)
        
    logger.info(f"Generated {len(windows)} windows")
    for i, (w_start, w_end) in enumerate(windows):
        logger.info(f"Window {i+1}: {w_start.isoformat()} to {w_end.isoformat()}")
        
    return windows


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
    window_config = WindowConfig(
        window_size=config.window_size,
        window_overlap=config.window_overlap
    )
    return calculate_window_bounds(start_time, end_time, window_config) 