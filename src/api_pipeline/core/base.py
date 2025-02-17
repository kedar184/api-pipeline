from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, AsyncIterator, Literal, Tuple
from enum import Enum
import asyncio
from datetime import datetime, UTC, timedelta
import aiohttp
from loguru import logger
from pydantic import BaseModel, Field
import random
import time
import backoff

from api_pipeline.core.auth import AuthConfig, create_auth_handler


class ProcessingPattern(str, Enum):
    """Defines how the extractor processes items.
    
    BATCH: Process multiple items, each requiring its own API call (e.g., /api/user/{id})
    SINGLE: Process one request that returns multiple items (e.g., /api/users?page=1)
    """
    BATCH = "batch"
    SINGLE = "single"


class PaginationType(str, Enum):
    """Supported pagination types."""
    PAGE_NUMBER = "page_number"  # e.g., ?page=1&per_page=100
    CURSOR = "cursor"           # e.g., ?cursor=abc123
    OFFSET = "offset"           # e.g., ?offset=100&limit=50
    TOKEN = "token"             # e.g., ?page_token=xyz789
    LINK = "link"               # Uses Link headers (like GitHub)


class ParallelProcessingStrategy(Enum):
    NONE = "none"
    TIME_WINDOWS = "time_windows"
    KNOWN_PAGES = "known_pages"
    CALCULATED_OFFSETS = "calculated_offsets"


class TimeWindowParallelConfig(BaseModel):
    """Configuration for time-based parallel processing"""
    window_size: str = "24h"  # e.g., "24h", "7d"
    window_overlap: str = "0m"  # overlap between windows
    max_concurrent_windows: int = 5
    timestamp_format: str = "%Y-%m-%dT%H:%M:%SZ"


class KnownPagesParallelConfig(BaseModel):
    """Configuration for page-based parallel processing"""
    max_concurrent_pages: int = 5
    require_total_count: bool = True
    safe_page_size: int = 100


class CalculatedOffsetParallelConfig(BaseModel):
    """Configuration for offset-based parallel processing"""
    max_concurrent_chunks: int = 5
    chunk_size: int = 1000
    require_total_count: bool = True


ParallelConfig = Union[
    TimeWindowParallelConfig,
    KnownPagesParallelConfig,
    CalculatedOffsetParallelConfig
]


class PaginationStrategy(ABC):
    """Abstract base class for pagination strategies."""
    
    @abstractmethod
    async def get_next_page_params(self, 
        current_params: Dict[str, Any],
        response: Dict[str, Any],
        page_number: int
    ) -> Optional[Dict[str, Any]]:
        """Get parameters for the next page based on current response.
        
        Args:
            current_params: Current request parameters
            response: Current page response
            page_number: Current page number (1-based)
            
        Returns:
            Parameters for next page, or None if no more pages
        """
        pass
    
    @abstractmethod
    def get_initial_params(self, base_params: Dict[str, Any], page_size: int) -> Dict[str, Any]:
        """Get parameters for the first page.
        
        Args:
            base_params: Base request parameters
            page_size: Number of items per page
            
        Returns:
            Parameters for first page
        """
        pass


class PageNumberStrategy(PaginationStrategy):
    """Strategy for page number based pagination."""
    
    def __init__(self, page_param: str = "page", size_param: str = "per_page"):
        self.page_param = page_param
        self.size_param = size_param
    
    async def get_next_page_params(self, 
        current_params: Dict[str, Any],
        response: Dict[str, Any],
        page_number: int
    ) -> Optional[Dict[str, Any]]:
        # If we got a full page, there might be more
        items = response if isinstance(response, list) else []
        if len(items) >= current_params[self.size_param]:
            params = current_params.copy()
            params[self.page_param] = page_number + 1
            return params
        return None
    
    def get_initial_params(self, base_params: Dict[str, Any], page_size: int) -> Dict[str, Any]:
        params = base_params.copy()
        params[self.page_param] = 1
        params[self.size_param] = page_size
        return params


class CursorStrategy(PaginationStrategy):
    """Strategy for cursor based pagination."""
    
    def __init__(self, cursor_param: str = "cursor", cursor_field: str = "next_cursor"):
        self.cursor_param = cursor_param
        self.cursor_field = cursor_field
    
    async def get_next_page_params(self,
        current_params: Dict[str, Any],
        response: Dict[str, Any],
        page_number: int
    ) -> Optional[Dict[str, Any]]:
        next_cursor = response.get(self.cursor_field)
        if next_cursor:
            params = current_params.copy()
            params[self.cursor_param] = next_cursor
            return params
        return None
    
    def get_initial_params(self, base_params: Dict[str, Any], page_size: int) -> Dict[str, Any]:
        return base_params.copy()  # Cursor usually doesn't need initial params


class LinkHeaderStrategy(PaginationStrategy):
    """Strategy for Link header based pagination (like GitHub)."""
    
    async def get_next_page_params(self,
        current_params: Dict[str, Any],
        response: Dict[str, Any],
        page_number: int
    ) -> Optional[Dict[str, Any]]:
        # Extract next link from Link header
        links = response.get("headers", {}).get("Link", "")
        for link in links.split(","):
            if 'rel="next"' in link:
                url = link.split(";")[0].strip()[1:-1]
                # Parse URL params into dict
                from urllib.parse import urlparse, parse_qs
                parsed = urlparse(url)
                params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                return params
        return None
    
    def get_initial_params(self, base_params: Dict[str, Any], page_size: int) -> Dict[str, Any]:
        return base_params.copy()


class OffsetStrategy(PaginationStrategy):
    """Strategy for offset based pagination."""
    
    def __init__(self, offset_param: str = "offset", limit_param: str = "limit"):
        self.offset_param = offset_param
        self.limit_param = limit_param
    
    async def get_next_page_params(self,
        current_params: Dict[str, Any],
        response: Dict[str, Any],
        page_number: int
    ) -> Optional[Dict[str, Any]]:
        current_offset = current_params.get(self.offset_param, 0)
        limit = current_params[self.limit_param]
        items = response if isinstance(response, list) else []
        
        if len(items) >= limit:
            params = current_params.copy()
            params[self.offset_param] = current_offset + limit
            return params
        return None
    
    def get_initial_params(self, base_params: Dict[str, Any], page_size: int) -> Dict[str, Any]:
        params = base_params.copy()
        params[self.offset_param] = 0
        params[self.limit_param] = page_size
        return params


class PaginationConfig(BaseModel):
    """Enhanced pagination configuration with strategy pattern."""
    enabled: bool = True
    strategy: PaginationStrategy
    page_size: int = 100
    max_pages: Optional[int] = None
    
    @classmethod
    def with_page_numbers(cls, page_size: int = 100, **kwargs) -> 'PaginationConfig':
        """Factory method for page number pagination."""
        return cls(
            strategy=PageNumberStrategy(**kwargs),
            page_size=page_size
        )
    
    @classmethod
    def with_cursor(cls, page_size: int = 100, **kwargs) -> 'PaginationConfig':
        """Factory method for cursor-based pagination."""
        return cls(
            strategy=CursorStrategy(**kwargs),
            page_size=page_size
        )
    
    @classmethod
    def with_offset(cls, page_size: int = 100, **kwargs) -> 'PaginationConfig':
        """Factory method for offset-based pagination."""
        return cls(
            strategy=OffsetStrategy(**kwargs),
            page_size=page_size
        )
    
    @classmethod
    def with_link_header(cls, page_size: int = 100) -> 'PaginationConfig':
        """Factory method for Link header pagination."""
        return cls(
            strategy=LinkHeaderStrategy(),
            page_size=page_size
        )


class RetryConfig(BaseModel):
    """Configuration for retry mechanism."""
    max_attempts: int = 3
    max_time: int = 30
    base_delay: float = 1.0
    max_delay: float = 10.0
    jitter: bool = True
    retry_on: List[int] = [429, 503, 504]


class WindowType(str, Enum):
    """Supported window types for batch processing."""
    FIXED = "fixed"      # Fixed-size time windows
    SLIDING = "sliding"  # Sliding windows with overlap
    SESSION = "session"  # Session-based windows


class WindowConfig(BaseModel):
    """Configuration for time-based windowing."""
    window_type: WindowType = WindowType.FIXED
    window_size: str = "1h"  # Duration string (e.g., "1h", "1d", "7d")
    window_offset: str = "0m"  # Offset for window start
    window_overlap: str = "0m"  # For sliding windows
    timestamp_field: str = "timestamp"  # Field to use for windowing
    
    @property
    def window_size_seconds(self) -> int:
        """Convert window size string to seconds."""
        return self._parse_duration(self.window_size)
    
    @property
    def window_offset_seconds(self) -> int:
        """Convert window offset string to seconds."""
        return self._parse_duration(self.window_offset)
    
    @property
    def window_overlap_seconds(self) -> int:
        """Convert window overlap string to seconds."""
        return self._parse_duration(self.window_overlap)
    
    def _parse_duration(self, duration: str) -> int:
        """Parse duration string to seconds."""
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
    """Configuration for watermark-based extraction."""
    enabled: bool = False
    timestamp_field: str = "updated_at"  # Field to track for watermark
    watermark_field: str = "last_watermark"  # Field to store watermark value
    window: Optional[WindowConfig] = None
    initial_watermark: Optional[datetime] = None
    lookback_window: str = "0m"  # How far to look back from watermark
    
    # Time range parameter names for API requests
    start_time_param: str = "start_time"  # Parameter name for start time
    end_time_param: str = "end_time"      # Parameter name for end time
    time_format: str = "%Y-%m-%dT%H:%M:%SZ"  # Format for time parameters
    
    @property
    def lookback_seconds(self) -> int:
        """Convert lookback window to seconds."""
        if not self.lookback_window:
            return 0
        return WindowConfig._parse_duration(self.lookback_window)


class ExtractorConfig(BaseModel):
    base_url: str
    endpoints: Dict[str, str]  # URL templates with {param} placeholders
    auth_config: AuthConfig
    processing_pattern: ProcessingPattern = ProcessingPattern.SINGLE
    batch_parameter_name: Optional[str] = None
    rate_limit: Optional[int] = None
    retry_count: Optional[int] = None
    batch_size: int = 100
    max_concurrent_requests: int = 10
    session_timeout: int = 30
    pagination: Optional[PaginationConfig] = None
    retry: RetryConfig = RetryConfig()
    watermark: Optional[WatermarkConfig] = None


class BaseExtractor(ABC):
    """Base class for all extractors with built-in performance optimizations.
    
    Features:
    - Concurrent request processing
    - Automatic batching
    - Connection pooling
    - Rate limiting
    - Resource management
    - Retry mechanism
    - Metrics tracking
    - Watermark-based extraction
    - Fixed window batching
    """
    
    # Custom exception classes
    class ExtractorError(Exception):
        """Base class for extractor errors."""
        pass

    class AuthenticationError(ExtractorError):
        """Raised when authentication fails."""
        pass

    class RateLimitError(ExtractorError):
        """Raised when rate limit is exceeded."""
        pass

    class ValidationError(ExtractorError):
        """Raised when parameters or data validation fails."""
        pass

    class WatermarkError(ExtractorError):
        """Raised when watermark handling fails."""
        pass

    DEFAULT_PAGE_SIZE = 100

    def __init__(self, config: ExtractorConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.auth_handler = create_auth_handler(config.auth_config)
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        self._rate_limiter = asyncio.Semaphore(config.rate_limit or float('inf'))
        self._retry_state: Dict[str, Any] = {}
        self._current_watermark: Optional[datetime] = None
        self._metrics = {
            'requests_made': 0,
            'requests_failed': 0,
            'items_processed': 0,
            'retry_attempts': 0,
            'total_processing_time': 0.0,
            'rate_limit_hits': 0,
            'auth_failures': 0,
            'watermark_updates': 0,
            'window_count': 0
        }
        self._start_time = time.monotonic()

    async def __aenter__(self) -> 'BaseExtractor':
        """Async context manager entry."""
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit with proper cleanup."""
        await self.cleanup()

    async def cleanup(self) -> None:
        """Clean up resources."""
        if self.session:
            await self.session.close()
            self.session = None
        self._retry_state.clear()

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics for monitoring."""
        current_time = time.monotonic()
        metrics = self._metrics.copy()
        metrics['uptime'] = current_time - self._start_time
        metrics['requests_per_second'] = (
            metrics['requests_made'] / metrics['uptime']
            if metrics['uptime'] > 0 else 0
        )
        metrics['success_rate'] = (
            (metrics['requests_made'] - metrics['requests_failed']) / metrics['requests_made']
            if metrics['requests_made'] > 0 else 0
        )
        return metrics

    async def _ensure_session(self):
        """Initialize session with authentication and headers."""
        if not self.session:
            # Get auth headers from auth handler
            self.auth_handler = create_auth_handler(self.config.auth_config)
            headers = await self.auth_handler.get_auth_headers()
            # Allow subclasses to add specific headers
            headers.update(self._get_additional_headers())
            self.session = aiohttp.ClientSession(headers=headers)

    def _get_additional_headers(self) -> Dict[str, str]:
        """Get additional headers for the API. Override in subclass if needed."""
        return {}

    @abstractmethod
    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single item from the API response.
        
        Args:
            item: Raw item from the API response
            
        Returns:
            Transformed item ready for output
            
        Raises:
            ValidationError: If item fails validation
            ExtractorError: For other transformation errors
        """
        pass

    @abstractmethod
    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate extraction parameters.
        
        Args:
            parameters: Parameters to validate
            
        Raises:
            ValidationError: If parameters are invalid
        """
        pass

    async def _process_batch(
        self,
        batch: List[Any],
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Process a batch of items concurrently."""
        async def process_item(item: Any) -> List[Dict[str, Any]]:
            try:
                async with self._semaphore:
                    data = await self._paginated_request(
                        "current",
                        params={**(params or {}), **self._get_item_params(item)}
                    )
                    items = data if isinstance(data, list) else [data]
                    return [await self._transform_item(item) for item in items]
            except Exception as e:
                logger.error(f"Failed to process item {item}: {str(e)}")
                return []

        tasks = [process_item(item) for item in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        processed_data = []
        for result in results:
            if isinstance(result, list):
                processed_data.extend(result)
            else:
                logger.error(f"Batch processing error: {str(result)}")
        
        return processed_data

    def _get_item_params(self, item: Any) -> Dict[str, Any]:
        """Get request parameters for an item. Override in subclass if needed."""
        return {"id": item} if isinstance(item, (str, int)) else item

    async def _paginated_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        endpoint_override: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Make paginated requests using the configured pagination strategy."""
        await self._ensure_session()
        all_results = []
        
        if not self.config.pagination or not self.config.pagination.enabled:
            response = await self._make_request(endpoint, params, endpoint_override)
            return self._extract_items(response)

        # Get initial parameters from the strategy
        page_params = self.config.pagination.strategy.get_initial_params(
            params or {}, 
            self.config.pagination.page_size
        )
        page_count = 0

        logger.info(f"Starting paginated request with strategy: {self.config.pagination.strategy.__class__.__name__}")
        logger.info(f"Initial parameters: {page_params}")

        while True:
            try:
                logger.info(f"Making request for page {page_count + 1} with params: {page_params}")
                async with self._semaphore:
                    response = await self._make_request(endpoint, page_params, endpoint_override)
                    
                items = self._extract_items(response)
                logger.info(f"Received {len(items)} items in page {page_count + 1}")
                all_results.extend(items)
                page_count += 1

                # Get next page parameters from the strategy
                next_params = await self.config.pagination.strategy.get_next_page_params(
                    page_params,
                    response,
                    page_count
                )
                
                # If no next parameters, we're done
                if not next_params or (
                    self.config.pagination.max_pages and 
                    page_count >= self.config.pagination.max_pages
                ):
                    break

                page_params = next_params

            except Exception as e:
                logger.error(f"Pagination request failed: {str(e)}")
                break

        logger.info(f"Completed pagination with {page_count} pages and {len(all_results)} total items")
        return all_results

    def _extract_items(self, response: Any) -> List[Dict[str, Any]]:
        """Extract items from response based on configuration."""
        if not self.config.pagination:
            return response if isinstance(response, list) else [response]

        if isinstance(response, list):
            return response

        items_field = self.config.pagination.items_field
        items = response.get(items_field, response)
        return items if isinstance(items, list) else [items]

    def _get_next_token(self, response: Dict[str, Any], pagination: PaginationConfig) -> Optional[str]:
        """Get next token based on pagination strategy."""
        if pagination.strategy == PaginationType.CURSOR:
            return response.get(pagination.cursor_field)
        elif pagination.strategy == PaginationType.TOKEN:
            return response.get(pagination.next_token_field)
        elif pagination.strategy == PaginationType.LINK:
            # Extract next link from Link header
            links = self.session.headers.get("Link", "")
            for link in links.split(","):
                if 'rel="next"' in link:
                    return link.split(";")[0].strip()[1:-1]
        return None

    def _should_continue_pagination(
        self,
        response: Dict[str, Any],
        items: List[Dict[str, Any]],
        page_count: int,
        next_token: Optional[str],
        pagination: PaginationConfig
    ) -> bool:
        """Determine if pagination should continue."""
        # Check max pages limit
        if pagination.max_pages and page_count >= pagination.max_pages:
            return False

        # Strategy-specific checks
        if pagination.strategy in [PaginationType.CURSOR, PaginationType.TOKEN]:
            return bool(next_token)
        elif pagination.strategy == PaginationType.PAGE_NUMBER:
            if pagination.has_more_field:
                return response.get(pagination.has_more_field, False)
            return len(items) >= pagination.page_size
        elif pagination.strategy == PaginationType.OFFSET:
            if pagination.total_field:
                total = response.get(pagination.total_field, 0)
                return (page_count * pagination.page_size) < total
            return len(items) >= pagination.page_size
        elif pagination.strategy == PaginationType.LINK:
            return bool(next_token)

        return False

    def _get_window_bounds(self, start_time: datetime) -> List[Tuple[datetime, datetime]]:
        """Calculate window bounds for the extraction period."""
        if not self.config.watermark or not self.config.watermark.window:
            logger.warning("No window configuration, using single window")
            return [(start_time, datetime.now(UTC))]
            
        window_config = self.config.watermark.window
        window_size = window_config.window_size_seconds
        current_time = datetime.now(UTC)
        
        logger.info(f"Calculating windows from {start_time.isoformat()} to {current_time.isoformat()}")
        logger.info(f"Window size: {window_size} seconds")
        
        windows = []
        window_start = start_time
        
        while window_start < current_time:
            window_end = min(
                window_start + timedelta(seconds=window_size),
                current_time
            )
            windows.append((window_start, window_end))
            window_start = window_end
            
        logger.info(f"Generated {len(windows)} windows")
        for i, (w_start, w_end) in enumerate(windows):
            logger.info(f"Window {i+1}: {w_start.isoformat()} to {w_end.isoformat()}")
            
        return windows

    async def _get_last_watermark(self) -> Optional[datetime]:
        """Get the last watermark value from storage."""
        if not hasattr(self, '_watermark_store'):
            self._watermark_store = {}
        
        key = self._get_watermark_key()
        stored_watermark = self._watermark_store.get(key)
        
        if stored_watermark:
            return stored_watermark
        
        if self.config.watermark and self.config.watermark.initial_watermark:
            return self.config.watermark.initial_watermark
            
        # Default to configured lookback if no watermark
        return datetime.now(UTC) - timedelta(
            seconds=self.config.watermark.lookback_seconds
        )

    def _get_watermark_key(self) -> str:
        """Get key for watermark storage. Override in subclass if needed."""
        return "default"

    async def _update_watermark(self, new_watermark: datetime) -> None:
        """Update the watermark value in storage."""
        key = self._get_watermark_key()
        self._watermark_store[key] = new_watermark
        self._metrics['watermark_updates'] += 1
        logger.info(f"Updated watermark for {key} to {new_watermark.isoformat()}")

    def _apply_watermark_filters(
        self,
        params: Dict[str, Any],
        window_start: datetime,
        window_end: datetime
    ) -> Dict[str, Any]:
        """Apply watermark-based filters to request parameters."""
        if not self.config.watermark or not self.config.watermark.enabled:
            logger.warning("Watermark filtering is disabled")
            return params
            
        params = {**params} if params else {}
        
        # Use configured parameter names and time format
        watermark = self.config.watermark
        logger.info(f"Applying watermark filters with config: start_param={watermark.start_time_param}, end_param={watermark.end_time_param}")
        logger.info(f"Window bounds: {window_start.isoformat()} to {window_end.isoformat()}")
        
        params[watermark.start_time_param] = window_start.strftime(watermark.time_format)
        params[watermark.end_time_param] = window_end.strftime(watermark.time_format)
        
        logger.info(f"Final parameters after watermark filters: {params}")
        return params

    async def extract(self, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Extract data with built-in batching, parallel processing, and watermark support.
        Override _transform_item() instead of this method.
        """
        try:
            parameters = parameters or {}
            
            # Get last watermark if enabled
            if self.config.watermark and self.config.watermark.enabled:
                self._current_watermark = await self._get_last_watermark()
                start_time = self._current_watermark
                if not start_time:
                    start_time = (
                        self.config.watermark.initial_watermark or 
                        datetime.now(UTC) - timedelta(
                            seconds=self.config.watermark.lookback_seconds
                        )
                    )
                
                # Calculate window bounds
                windows = self._get_window_bounds(start_time)
                logger.info(f"Processing {len(windows)} windows from {start_time}")
                
                all_data = []
                max_watermark = start_time
                
                # Process each window
                for window_start, window_end in windows:
                    self._metrics['window_count'] += 1
                    logger.info(f"Processing window: {window_start} to {window_end}")
                    
                    # Apply watermark filters
                    window_params = self._apply_watermark_filters(
                        parameters, window_start, window_end
                    )
                    
                    # Process items in window
                    items = self._get_items_to_process(window_params)
                    if not items:
                        continue
                    
                    # Process items in batches
                    for i in range(0, len(items), self.config.batch_size):
                        batch = items[i:i + self.config.batch_size]
                        batch_data = await self._process_batch(batch, window_params)
                        all_data.extend(batch_data)
                        
                        # Update watermark if needed
                        for item in batch_data:
                            item_timestamp = datetime.fromisoformat(
                                str(item[self.config.watermark.timestamp_field])
                            )
                            max_watermark = max(max_watermark, item_timestamp)
                
                # Update final watermark
                if max_watermark > start_time:
                    await self._update_watermark(max_watermark)
                
                return all_data
                
            else:
                # Non-watermark based extraction (existing implementation)
                items = self._get_items_to_process(parameters)
                if not items:
                    logger.warning("No items to process")
                    return []

                logger.info(f"Processing {len(items)} items in batches of {self.config.batch_size}")
                all_data = []
                
                for i in range(0, len(items), self.config.batch_size):
                    batch = items[i:i + self.config.batch_size]
                    batch_data = await self._process_batch(batch, parameters)
                    all_data.extend(batch_data)
                    
                    logger.info(f"Processed batch {i//self.config.batch_size + 1}, "
                              f"total items processed: {len(all_data)}")
                
                return all_data
            
        finally:
            if self.session:
                await self.session.close()
                self.session = None

    def _get_items_to_process(self, parameters: Dict[str, Any]) -> List[Any]:
        """Get items to process based on the configured processing pattern.
        
        For BATCH pattern:
            Returns the list of items from the configured batch_parameter_name.
            Each item will get its own API call.
            
        For SINGLE pattern:
            Returns [parameters] to make a single API call that returns multiple items.
            
        Override this method if you need custom item processing logic.
        """
        if self.config.processing_pattern == ProcessingPattern.BATCH:
            if not self.config.batch_parameter_name:
                raise ValueError("batch_parameter_name must be set for BATCH processing pattern")
                
            items = parameters.get(self.config.batch_parameter_name)
            if not items:
                logger.warning(f"No items found in parameter '{self.config.batch_parameter_name}'")
                return []
                
            if not isinstance(items, list):
                raise ValueError(f"Parameter '{self.config.batch_parameter_name}' must be a list")
                
            return items
            
        # SINGLE pattern - use parameters as is
        return [parameters]

    def _get_url(self, endpoint: str, params: Dict[str, Any], endpoint_override: Optional[str] = None) -> str:
        """Construct the full URL for a request.
        
        Handles URL template parameters in the format {param_name}.
        Example: "/repos/{repo}/commits" with params={"repo": "apache/spark"}
        becomes "/repos/apache/spark/commits"
        
        Args:
            endpoint: The endpoint key from config.endpoints
            params: Request parameters, used both for URL template and query params
            endpoint_override: Optional override for the entire endpoint path
            
        Returns:
            The complete URL with template parameters replaced
            
        Raises:
            ValueError: If a template parameter is missing from params
        """
        if endpoint_override:
            path = endpoint_override
        else:
            path = self.config.endpoints[endpoint]
            
        try:
            # Format the URL template with provided parameters
            path = path.format(**params)
        except KeyError as e:
            raise ValueError(f"Missing required URL parameter: {e}")
            
        return f"{self.config.base_url}{path}"

    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        endpoint_override: Optional[str] = None
    ) -> Dict[str, Any]:
        """Make a single request with retries and rate limiting."""
        params = params or {}
        url = self._get_url(endpoint, params, endpoint_override)
        
        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()

    def _get_retry_state(self, request_id: str) -> Dict[str, Any]:
        """Get or create retry state for a request."""
        if request_id not in self._retry_state:
            self._retry_state[request_id] = {
                'attempts': 0,
                'start_time': time.monotonic(),
                'last_error': None
            }
        return self._retry_state[request_id]

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay with jitter."""
        delay = min(
            self.config.retry.base_delay * (2 ** attempt),
            self.config.retry.max_delay
        )
        if self.config.retry.jitter:
            delay *= (1 + random.random())
        return delay

    def _should_retry(self, status_code: int) -> bool:
        """Determine if request should be retried based on status."""
        return (
            status_code in self.config.retry.retry_on or
            status_code >= 500
        )

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, TimeoutError),
        max_tries=lambda self: self.config.retry.max_attempts,
        max_time=lambda self: self.config.retry.max_time,
        on_backoff=lambda details: logger.warning(f"Retrying request: {details}")
    )
    async def _do_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make HTTP request with exponential backoff retry."""
        request_id = f"{endpoint}:{hash(str(params))}"
        state = self._get_retry_state(request_id)
        
        try:
            async with self.session.get(
                f"{self.config.base_url}{self.config.endpoints[endpoint]}",
                params=params
            ) as response:
                if response.status == 429:  # Rate limit
                    retry_after = int(response.headers.get('Retry-After', 5))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds")
                    await asyncio.sleep(retry_after)
                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=429
                    )
                
                response.raise_for_status()
                return await response.json()
                
        except aiohttp.ClientResponseError as e:
            state['attempts'] += 1
            state['last_error'] = str(e)
            
            if self._should_retry(e.status):
                if time.monotonic() - state['start_time'] < self.config.retry.max_time:
                    delay = self._calculate_delay(state['attempts'])
                    logger.warning(f"Request failed with {e.status}. Retrying in {delay:.2f}s")
                    await asyncio.sleep(delay)
                    raise  # Will be retried by backoff decorator
                else:
                    logger.error(f"Max retry time exceeded for {request_id}")
            
            logger.error(f"Request failed: {str(e)}")
            raise
            
        except (aiohttp.ClientError, TimeoutError) as e:
            state['attempts'] += 1
            state['last_error'] = str(e)
            
            if state['attempts'] < self.config.retry.max_attempts:
                delay = self._calculate_delay(state['attempts'])
                logger.warning(f"Network error. Retrying in {delay:.2f}s")
                await asyncio.sleep(delay)
                raise  # Will be retried by backoff decorator
            else:
                logger.error(f"Max retry attempts exceeded for {request_id}")
                raise
        
        finally:
            if state['attempts'] >= self.config.retry.max_attempts:
                self._retry_state.pop(request_id, None)

    def _get_endpoint_override(self, parameters: Dict[str, Any]) -> Optional[str]:
        """Get the endpoint override for the API request.
        Override this in subclasses to provide custom endpoint construction.
        
        Args:
            parameters: The parameters passed to the extractor
            
        Returns:
            Optional endpoint override string
        """
        return None

    async def _process_window(self, window_start: datetime, window_end: datetime, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process a single time window and return items."""
        window_start_time = time.time()
        self._metrics['window_count'] += 1
        logger.info(f"Starting window processing: {window_start} to {window_end}")
        
        # Apply watermark filters
        window_params = self._apply_watermark_filters(
            parameters, window_start, window_end
        )
        logger.info(f"Window parameters after watermark filters: {window_params}")
        
        # Get endpoint override if any
        endpoint_override = self._get_endpoint_override(parameters)
        logger.info(f"Using endpoint override: {endpoint_override}")
        
        # Fetch items for window using the first endpoint key
        endpoint_key = next(iter(self.config.endpoints.keys()))
        logger.info(f"Using endpoint key: {endpoint_key}")
        
        items = await self._paginated_request(
            endpoint_key,
            params=window_params,
            endpoint_override=endpoint_override
        )
        
        logger.info(f"Received {len(items) if items else 0} items from API")
        
        if not items:
            window_end_time = time.time()
            processing_time = window_end_time - window_start_time
            self._metrics['total_processing_time'] += processing_time
            logger.info(f"Window processing completed in {processing_time:.2f} seconds")
            return []
        
        # Transform items in parallel
        transformed_items = await asyncio.gather(
            *[self._transform_item(item) for item in items]
        )
        
        window_end_time = time.time()
        processing_time = window_end_time - window_start_time
        self._metrics['total_processing_time'] += processing_time
        self._metrics['items_processed'] += len(transformed_items)
        
        logger.info(f"Window processing completed in {processing_time:.2f} seconds")
        logger.info(f"Transformed {len(transformed_items)} items")
        if transformed_items:
            first_item = transformed_items[0]
            logger.info(f"Sample transformed item timestamp: {first_item.get(self.config.watermark.timestamp_field)}")
        
        return transformed_items

    async def _process_parallel(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process data in parallel based on the configured strategy"""
        if not self.config.pagination.parallel_strategy or \
           self.config.pagination.parallel_strategy == ParallelProcessingStrategy.NONE:
            return await self._process_sequential(parameters)

        if self.config.pagination.parallel_strategy == ParallelProcessingStrategy.TIME_WINDOWS:
            return await self._process_time_windows(parameters)
        elif self.config.pagination.parallel_strategy == ParallelProcessingStrategy.KNOWN_PAGES:
            return await self._process_known_pages(parameters)
        elif self.config.pagination.parallel_strategy == ParallelProcessingStrategy.CALCULATED_OFFSETS:
            return await self._process_calculated_offsets(parameters)
        
        raise ValueError(f"Unsupported parallel strategy: {self.config.pagination.parallel_strategy}")

    async def _process_time_windows(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process data using time-based windowing"""
        if not isinstance(self.config.pagination.parallel_config, TimeWindowParallelConfig):
            raise ValueError("Time window configuration required for time-based parallel processing")

        config = self.config.pagination.parallel_config
        start_time = self._get_start_time(parameters)
        end_time = self._get_end_time(parameters)
        
        # Calculate time windows
        windows = self._calculate_time_windows(start_time, end_time, config)
        logger.info(f"Processing {len(windows)} time windows from {start_time} to {end_time}")
        
        # Process windows in parallel with concurrency limit
        semaphore = asyncio.Semaphore(config.max_concurrent_windows)
        async def process_window_with_semaphore(window_start, window_end):
            async with semaphore:
                window_params = parameters.copy()
                window_params[self.config.pagination.start_time_param] = window_start.strftime(config.timestamp_format)
                window_params[self.config.pagination.end_time_param] = window_end.strftime(config.timestamp_format)
                return await self._process_sequential(window_params)
        
        tasks = [
            process_window_with_semaphore(start, end)
            for start, end in windows
        ]
        
        results = await asyncio.gather(*tasks)
        return [item for sublist in results for item in sublist]  # Flatten results

    def _calculate_time_windows(
        self, 
        start_time: datetime, 
        end_time: datetime, 
        config: TimeWindowParallelConfig
    ) -> List[Tuple[datetime, datetime]]:
        """Calculate time windows for parallel processing"""
        window_size = self.config.pagination.parallel_config.window_size_seconds
        window_overlap = self.config.pagination.parallel_config.window_overlap_seconds
        
        windows = []
        current_start = start_time
        
        while current_start < end_time:
            window_end = min(current_start + window_size, end_time)
            windows.append((current_start, window_end))
            current_start = window_end - window_overlap
        
        return windows

    def _get_start_time(self, parameters: Dict[str, Any]) -> datetime:
        """Get start time from parameters or use default"""
        if self.config.pagination.start_time_param in parameters:
            return parse_datetime(parameters[self.config.pagination.start_time_param])
        return datetime.now(UTC) - timedelta(days=7)  # Default to last 7 days

    def _get_end_time(self, parameters: Dict[str, Any]) -> datetime:
        """Get end time from parameters or use default"""
        if self.config.pagination.end_time_param in parameters:
            return parse_datetime(parameters[self.config.pagination.end_time_param])
        return datetime.now(UTC)

    async def _process_known_pages(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process data using parallel page fetching"""
        # Implementation for page-based parallel processing
        raise NotImplementedError("Page-based parallel processing not implemented yet")

    async def _process_calculated_offsets(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process data using parallel offset fetching"""
        # Implementation for offset-based parallel processing
        raise NotImplementedError("Offset-based parallel processing not implemented yet")

    async def _process_sequential(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process data sequentially (existing pagination logic)"""
        # Move existing pagination logic here
        raise NotImplementedError("Sequential processing must be implemented by subclass")


class OutputConfig(BaseModel):
    type: str
    enabled: bool = True
    config: Dict[str, Any]


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
            'total_write_time': 0.0
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
            # Subclasses should implement actual writing logic
            self._metrics['records_written'] += len(data)
            self._metrics['last_write_time'] = datetime.now(UTC)
        except Exception as e:
            self._metrics['write_errors'] += 1
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
        metrics['uptime'] = current_time - self._start_time
        metrics['records_per_second'] = (
            metrics['records_written'] / metrics['uptime']
            if metrics['uptime'] > 0 else 0
        )
        metrics['average_write_time'] = (
            metrics['total_write_time'] / metrics['records_written']
            if metrics['records_written'] > 0 else 0
        )
        return metrics 