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


class PaginationType(str, Enum):
    """Supported pagination types."""
    PAGE_NUMBER = "page_number"  # e.g., ?page=1&per_page=100
    CURSOR = "cursor"           # e.g., ?cursor=abc123
    OFFSET = "offset"           # e.g., ?offset=100&limit=50
    TOKEN = "token"             # e.g., ?page_token=xyz789
    LINK = "link"               # Uses Link headers (like GitHub)


class PaginationConfig(BaseModel):
    """Configuration for API pagination."""
    enabled: bool = True
    strategy: PaginationType = PaginationType.PAGE_NUMBER
    page_size: int = 100
    max_pages: Optional[int] = None
    
    # Page number strategy
    page_param: str = "page"
    size_param: str = "per_page"
    
    # Cursor strategy
    cursor_param: str = "cursor"
    cursor_field: str = "next_cursor"
    
    # Offset strategy
    offset_param: str = "offset"
    limit_param: str = "limit"
    
    # Token strategy
    token_param: str = "page_token"
    next_token_field: str = "next_page_token"
    
    # Response parsing
    items_field: str = "items"  # Field containing items in response
    total_field: Optional[str] = None  # Field containing total items count
    has_more_field: Optional[str] = None  # Field indicating more pages


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
    endpoints: Dict[str, str]
    auth_config: AuthConfig
    rate_limit: Optional[int] = None
    retry_count: Optional[int] = None
    batch_size: int = 100
    max_concurrent_requests: int = 10
    session_timeout: int = 30
    pagination: Optional[PaginationConfig] = None
    retry: RetryConfig = RetryConfig()
    watermark: Optional[WatermarkConfig] = None  # Add watermark configuration


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
        """Make paginated requests with support for different pagination strategies."""
        await self._ensure_session()
        all_results = []
        
        if not self.config.pagination or not self.config.pagination.enabled:
            response = await self._make_request(endpoint, params, endpoint_override)
            return self._extract_items(response)

        pagination = self.config.pagination
        page_params = {**(params or {})}
        page_count = 0
        next_token = None

        logger.info(f"Starting paginated request with strategy: {pagination.strategy}")
        logger.info(f"Initial parameters: {page_params}")

        while True:
            # Update parameters based on pagination strategy
            if pagination.strategy == PaginationType.PAGE_NUMBER:
                page_params.update({
                    pagination.page_param: page_count + 1,
                    pagination.size_param: pagination.page_size
                })
            elif pagination.strategy == PaginationType.CURSOR and next_token:
                page_params[pagination.cursor_param] = next_token
            elif pagination.strategy == PaginationType.OFFSET:
                page_params.update({
                    pagination.offset_param: page_count * pagination.page_size,
                    pagination.limit_param: pagination.page_size
                })
            elif pagination.strategy == PaginationType.TOKEN and next_token:
                page_params[pagination.token_param] = next_token

            try:
                logger.info(f"Making request for page {page_count + 1} with params: {page_params}")
                async with self._semaphore:
                    response = await self._make_request(endpoint, page_params, endpoint_override)
                    
                items = self._extract_items(response)
                logger.info(f"Received {len(items)} items in page {page_count + 1}")
                all_results.extend(items)
                page_count += 1

                # Get next token based on strategy
                next_token = self._get_next_token(response, pagination)
                logger.info(f"Next token: {next_token}")
                
                # Check if we should continue
                should_continue = self._should_continue_pagination(
                    response, items, page_count, next_token, pagination
                )
                logger.info(f"Should continue pagination: {should_continue}")
                if not should_continue:
                    break

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
        """Get items to process from parameters. Override in subclass if needed."""
        # Default implementation looks for common parameter names
        for param in ['items', 'ids', 'locations', 'location_ids']:
            if param in parameters:
                return parameters[param]
        return []

    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        endpoint_override: Optional[str] = None
    ) -> Dict[str, Any]:
        """Make a single request with retries and rate limiting."""
        url = (f"{self.config.base_url}{endpoint_override}"
               if endpoint_override
               else f"{self.config.base_url}{self.config.endpoints[endpoint]}")
        
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