from datetime import datetime, timedelta, UTC
from typing import Dict, Any, Optional
from pydantic import BaseModel
from loguru import logger

from api_pipeline.core.windowing import WindowConfig


class WatermarkConfig(BaseModel):
    """Configuration for watermark-based incremental extraction."""
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
        """Convert lookback window string to seconds."""
        if not self.lookback_window:
            return 0
        return WindowConfig._parse_duration(self.lookback_window)


class WatermarkHandler:
    """Handles watermark state management and filtering."""
    
    def __init__(self, config: WatermarkConfig):
        self.config = config
        self._watermark_store: Dict[str, datetime] = {}
        self._metrics = {
            'watermark_updates': 0,
            'last_update_time': None
        }
    
    async def get_last_watermark(self, key: str = "default") -> Optional[datetime]:
        """Get the last watermark value from storage.
        
        Args:
            key: Key for watermark storage
            
        Returns:
            Last watermark value or initial watermark if none exists
        """
        stored_watermark = self._watermark_store.get(key)
        
        if stored_watermark:
            return stored_watermark
        
        if self.config.initial_watermark:
            return self.config.initial_watermark
            
        # Default to configured lookback if no watermark
        return datetime.now(UTC) - timedelta(seconds=self.config.lookback_seconds)
    
    async def update_watermark(self, new_watermark: datetime, key: str = "default") -> None:
        """Update the watermark value in storage.
        
        Args:
            new_watermark: New watermark value
            key: Key for watermark storage
        """
        self._watermark_store[key] = new_watermark
        self._metrics['watermark_updates'] += 1
        self._metrics['last_update_time'] = datetime.now(UTC)
        logger.info(f"Updated watermark for {key} to {new_watermark.isoformat()}")
    
    def apply_watermark_filters(
        self,
        params: Dict[str, Any],
        window_start: datetime,
        window_end: datetime
    ) -> Dict[str, Any]:
        """Apply watermark-based filters to request parameters.
        
        Args:
            params: Original request parameters
            window_start: Start of the time window
            window_end: End of the time window
            
        Returns:
            Updated parameters with watermark filters applied
        """
        if not self.config.enabled:
            logger.warning("Watermark filtering is disabled")
            return params
            
        params = {**params} if params else {}
        
        logger.info(f"Applying watermark filters with config: start_param={self.config.start_time_param}, end_param={self.config.end_time_param}")
        logger.info(f"Window bounds: {window_start.isoformat()} to {window_end.isoformat()}")
        
        params[self.config.start_time_param] = window_start.strftime(self.config.time_format)
        params[self.config.end_time_param] = window_end.strftime(self.config.time_format)
        
        logger.info(f"Final parameters after watermark filters: {params}")
        return params
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get watermark-related metrics."""
        return self._metrics.copy() 