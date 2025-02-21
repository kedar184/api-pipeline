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

from api_pipeline.core.auth import AuthConfig, create_auth_handler
from api_pipeline.core.types import (
    ProcessingPattern,
    PaginationType,
    ParallelProcessingStrategy,
    WindowType
)
from api_pipeline.core.pagination import (
    PaginationConfig,
    PaginationStrategy,
    PageNumberConfig,
    CursorConfig,
    OffsetConfig,
    LinkHeaderConfig,
    PageNumberStrategy,
    CursorStrategy,
    LinkHeaderStrategy,
    OffsetStrategy,
    PaginationStrategyConfig
)
from api_pipeline.core.windowing import (
    WindowConfig,
    calculate_window_bounds
)
from api_pipeline.core.watermark import (
    WatermarkConfig,
    WatermarkHandler
)
from api_pipeline.core.retry import RetryConfig, RetryHandler
from api_pipeline.core.parallel import (
    TimeWindowParallelConfig,
    KnownPagesParallelConfig,
    CalculatedOffsetParallelConfig,
    ParallelConfig
)


class ExtractorConfig(BaseModel):
    # Main configuration for API data extraction
    base_url: str                                        # Base URL for API
    endpoints: Dict[str, str]                           # URL templates with {param} placeholders
    auth_config: AuthConfig                             # Authentication configuration
    processing_pattern: ProcessingPattern = ProcessingPattern.SINGLE  # How to process items
    batch_parameter_name: Optional[str] = None          # Parameter name for batch items
    rate_limit: Optional[int] = None                    # Maximum requests per second
    retry_count: Optional[int] = None                   # Number of retries
    batch_size: int = 100                              # Items per batch
    max_concurrent_requests: int = 10                   # Maximum parallel requests
    session_timeout: int = 30                          # Request timeout in seconds
    pagination: Optional[PaginationConfig] = None       # Pagination configuration
    retry: RetryConfig = RetryConfig()                 # Retry configuration
    watermark: Optional[WatermarkConfig] = None        # Watermark configuration


class BaseExtractor(ABC):
    # Base class for API data extractors with built-in optimizations:
    # - Concurrent request processing
    # - Automatic batching
    # - Connection pooling
    # - Rate limiting
    # - Resource management
    # - Retry mechanism
    # - Metrics tracking
    # - Watermark-based extraction
    # - Fixed window batching
    
    # Custom exception classes for different error scenarios
    class ExtractorError(Exception):
        # Base class for extractor errors
        pass

    class AuthenticationError(ExtractorError):
        # Raised when authentication fails
        pass

    class RateLimitError(ExtractorError):
        # Raised when rate limit is exceeded
        pass

    class ValidationError(ExtractorError):
        # Raised when parameters or data validation fails
        pass

    class WatermarkError(ExtractorError):
        # Raised when watermark handling fails
        pass

    DEFAULT_PAGE_SIZE = 100

    def __init__(self, config: ExtractorConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.auth_handler = create_auth_handler(config.auth_config)
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        self._rate_limiter = asyncio.Semaphore(config.rate_limit or float('inf'))
        self.retry_handler = RetryHandler(config.retry)
        if self.config.watermark:
            self.watermark_handler = WatermarkHandler(self.config.watermark)
        self._metrics = {
            'requests_made': 0,
            'requests_failed': 0,
            'items_processed': 0,
            'retry_attempts': 0,
            'total_processing_time': 0.0,
            'rate_limit_hits': 0,
            'auth_failures': 0,
            'window_count': 0,
            'batch_count': 0,
            'failed_batches': 0,
            'bytes_processed': 0,
            'last_request_time': None,
            'average_batch_size': 0,
            'max_batch_size': 0,
            'min_batch_size': float('inf'),
            'total_batch_size': 0,
            'watermark_updates': 0
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

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics for monitoring."""
        current_time = time.monotonic()
        metrics = self._metrics.copy()
        uptime = current_time - self._start_time
        
        # Calculate derived metrics
        metrics['uptime'] = uptime
        metrics['requests_per_second'] = (
            metrics['requests_made'] / uptime
            if uptime > 0 else 0
        )
        metrics['success_rate'] = (
            (metrics['requests_made'] - metrics['requests_failed']) / metrics['requests_made']
            if metrics['requests_made'] > 0 else 0
        )
        metrics['average_processing_time_per_item'] = (
            metrics['total_processing_time'] / metrics['items_processed']
            if metrics['items_processed'] > 0 else 0
        )
        metrics['batch_success_rate'] = (
            (metrics['batch_count'] - metrics['failed_batches']) / metrics['batch_count']
            if metrics['batch_count'] > 0 else 0
        )
        metrics['average_batch_size'] = (
            metrics['items_processed'] / metrics['batch_count']
            if metrics['batch_count'] > 0 else 0
        )
        
        # Add auth metrics if available
        if hasattr(self, 'auth_handler'):
            auth_metrics = self.auth_handler.get_metrics()
            metrics.update(auth_metrics)
        
        # Add retry metrics if available
        if hasattr(self, 'retry_handler'):
            retry_metrics = self.retry_handler.get_metrics()
            metrics.update(retry_metrics)
        
        # Add watermark metrics if available
        if hasattr(self, 'watermark_handler'):
            watermark_metrics = self.watermark_handler.get_metrics()
            metrics.update(watermark_metrics)
        
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
        # Get additional headers for API requests
        # Override in subclass to add custom headers
        # Returns: Dictionary of header name to value
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
        self._metrics['batch_count'] += 1
        batch_start_time = time.monotonic()
        
        # Get first endpoint from config
        endpoint = next(iter(self.config.endpoints.keys()))
        logger.debug(f"Using endpoint: {endpoint}")
        
        async def process_item(item: Any) -> List[Dict[str, Any]]:
            try:
                async with self._semaphore:
                    data = await self._paginated_request(
                        endpoint,
                        params={**(params or {}), **self._get_item_params(item)}
                    )
                    items = data if isinstance(data, list) else [data]
                    transformed_items = [await self._transform_item(item) for item in items]
                    self._metrics['items_processed'] += len(transformed_items)
                    return transformed_items
            except Exception as e:
                logger.error(f"Failed to process item {item}: {str(e)}")
                self._metrics['failed_batches'] += 1
                return []

        tasks = [process_item(item) for item in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        processed_data = []
        for result in results:
            if isinstance(result, list):
                processed_data.extend(result)
            else:
                logger.error(f"Batch processing error: {str(result)}")
                self._metrics['failed_batches'] += 1
        
        # Update batch metrics
        batch_time = time.monotonic() - batch_start_time
        self._metrics['total_processing_time'] += batch_time
        self._metrics['total_batch_size'] += len(processed_data)
        self._metrics['max_batch_size'] = max(self._metrics['max_batch_size'], len(processed_data))
        if len(processed_data) > 0:
            self._metrics['min_batch_size'] = min(
                self._metrics['min_batch_size'] if self._metrics['min_batch_size'] != float('inf') else len(processed_data),
                len(processed_data)
            )
        
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
        """Make a paginated request and return all results."""
        if not self.config.pagination or not self.config.pagination.enabled:
            response = await self._make_request(endpoint, params, endpoint_override)
            items = self._extract_items(response)
            self._metrics['requests_made'] += 1
            self._metrics['bytes_processed'] += len(str(response).encode())
            return items

        # Get initial parameters from the strategy
        page_params = self.config.pagination.strategy.get_initial_params(
            params or {}, 
            self.config.pagination.page_size
        )
        page_count = 0
        all_results = []
        request_start_time = time.monotonic()

        logger.info(f"Starting paginated request with strategy: {self.config.pagination.strategy.__class__.__name__}")
        logger.info(f"Initial parameters: {page_params}")

        while True:
            # Check max_pages limit before making the request
            if self.config.pagination.max_pages and page_count >= self.config.pagination.max_pages:
                logger.info(f"Reached max pages limit ({self.config.pagination.max_pages}), stopping pagination")
                break

            try:
                logger.info(f"Making request for page {page_count + 1} with params: {page_params}")
                async with self._semaphore:
                    response = await self._make_request(endpoint, page_params, endpoint_override)
                    self._metrics['requests_made'] += 1
                    self._metrics['bytes_processed'] += len(str(response).encode())
                    
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
                if not next_params:
                    break

                page_params = next_params

            except Exception as e:
                logger.error(f"Pagination request failed: {str(e)}")
                self._metrics['requests_failed'] += 1
                break

        # Update request metrics
        request_time = time.monotonic() - request_start_time
        self._metrics['total_processing_time'] += request_time
        self._metrics['last_request_time'] = datetime.now(UTC)
        
        logger.info(f"Completed pagination with {page_count} pages and {len(all_results)} total items")
        return all_results

    def _extract_items(self, response: Any) -> List[Dict[str, Any]]:
        """Extract items from response based on configuration."""
        if not self.config.pagination:
            return response if isinstance(response, list) else [response]

        if isinstance(response, list):
            return response

        # For dict responses, check if it's a paginated response with headers
        if isinstance(response, dict):
            # If response has 'data' field (our wrapper), use that
            if 'data' in response:
                items = response['data']
            # Otherwise use the response itself (excluding headers)
            else:
                items = {k: v for k, v in response.items() if k != 'headers'}
            return items if isinstance(items, list) else [items]

        return [response]

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
        """Extract data with built-in batching, parallel processing, and watermark support."""
        try:
            parameters = parameters or {}
            
            # Get last watermark if enabled
            if self.config.watermark and self.config.watermark.enabled:
                start_time = await self.watermark_handler.get_last_watermark()
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
                    window_params = self.watermark_handler.apply_watermark_filters(
                        parameters, window_start, window_end
                    )
                    
                    # Process items in window
                    items = await self._get_items_to_process(window_params)
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
                    await self.watermark_handler.update_watermark(max_watermark)
                
                return all_data
                
            else:
                # Non-watermark based extraction
                items = await self._get_items_to_process(parameters)
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

    async def _get_items_to_process(self, parameters: Dict[str, Any]) -> List[Any]:
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
        """Get full URL with parameters.
        
        Args:
            endpoint: Endpoint key from configuration
            params: Request parameters
            endpoint_override: Optional override for endpoint URL
            
        Returns:
            Complete URL with parameters
        """
        # Get endpoint URL
        if endpoint_override:
            url = endpoint_override
        else:
            if endpoint not in self.config.endpoints:
                raise ValueError(f"Endpoint {endpoint} not found in configuration")
            url = self.config.endpoints[endpoint]
            
        # Add base URL if endpoint doesn't start with http
        if not url.startswith(('http://', 'https://')):
            url = f"{self.config.base_url.rstrip('/')}/{url.lstrip('/')}"
            
        # Add parameters
        query_params = {}
        
        # Add configured parameters if any
        if hasattr(self.config, 'parameters'):
            query_params.update(self.config.parameters)
            
        # Add units parameter if present
        if hasattr(self.config, 'units'):
            query_params['units'] = self.config.units
            
        # Add request parameters
        query_params.update(params)
        
        # Build query string
        if query_params:
            param_strings = []
            for key, value in sorted(query_params.items()):
                if isinstance(value, (list, tuple)):
                    param_strings.extend(f"{key}={v}" for v in value)
                else:
                    param_strings.append(f"{key}={value}")
            url = f"{url}{'&' if '?' in url else '?'}{'&'.join(param_strings)}"
            
        return url

    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        endpoint_override: Optional[str] = None
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic."""
        await self._ensure_session()
        
        async def do_request():
            url = self._get_url(endpoint, params or {}, endpoint_override)
            # Add additional parameters from config if available
            request_params = {k: v for k, v in (params or {}).items() 
                            if isinstance(v, (str, int, float, bool))}
            
            # Add API key as query parameter if using api_key auth
            if hasattr(self.auth_handler, 'get_auth_params'):
                request_params.update(self.auth_handler.get_auth_params())
                logger.debug(f"Added auth params: {request_params}")
            
            # Get headers including auth headers
            headers = await self.auth_handler.get_auth_headers()
            logger.debug(f"Request headers: {headers}")
            
            async with self.session.get(url, params=request_params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
                # Include headers in the response for pagination
                if isinstance(data, dict):
                    data['headers'] = dict(response.headers)
                    return data
                return {
                    'data': data,
                    'headers': dict(response.headers)
                }
        
        # Create retry handler for this request
        retry_handler = RetryHandler(self.config.retry)
        request_id = f"{endpoint}:{hash(str(params))}"
        return await retry_handler.execute_with_retry(request_id, do_request)

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
        # Process data using parallel page fetching when total pages are known
        # This strategy works best when API provides total count/pages information
        # Returns: Combined results from all parallel page fetches
        
        if not isinstance(self.config.pagination.parallel_config, KnownPagesParallelConfig):
            raise ValueError("Known pages configuration required for page-based parallel processing")
            
        config = self.config.pagination.parallel_config
        
        # Get total pages if API provides it
        first_page = await self._make_request(
            next(iter(self.config.endpoints.keys())),
            parameters
        )
        
        if config.require_total_count:
            if 'total' not in first_page:
                raise ValueError("API response missing required 'total' field")
            total_items = first_page['total']
            total_pages = (total_items + config.safe_page_size - 1) // config.safe_page_size
        else:
            # Start with first page and discover more as needed
            total_pages = 1
            
        # Process pages in parallel with concurrency limit
        semaphore = asyncio.Semaphore(config.max_concurrent_pages)
        
        async def fetch_page(page_number: int) -> List[Dict[str, Any]]:
            async with semaphore:
                page_params = parameters.copy()
                page_params['page'] = page_number
                page_params['per_page'] = config.safe_page_size
                response = await self._make_request(
                    next(iter(self.config.endpoints.keys())),
                    page_params
                )
                return self._extract_items(response)
                
        tasks = [fetch_page(page) for page in range(1, total_pages + 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine and filter results
        all_items = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Page processing failed: {str(result)}")
                continue
            all_items.extend(result)
            
        return all_items

    async def _process_calculated_offsets(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Process data using parallel offset-based fetching
        # This strategy works by calculating offset ranges based on chunk size
        # Returns: Combined results from all parallel chunk fetches
        
        if not isinstance(self.config.pagination.parallel_config, CalculatedOffsetParallelConfig):
            raise ValueError("Calculated offset configuration required for offset-based parallel processing")
            
        config = self.config.pagination.parallel_config
        
        # Get total count if required
        if config.require_total_count:
            first_response = await self._make_request(
                next(iter(self.config.endpoints.keys())),
                parameters
            )
            if 'total' not in first_response:
                raise ValueError("API response missing required 'total' field")
            total_items = first_response['total']
        else:
            # Start with one chunk and discover more as needed
            total_items = config.chunk_size
            
        # Calculate chunks
        chunks = []
        for offset in range(0, total_items, config.chunk_size):
            chunks.append((offset, min(offset + config.chunk_size, total_items)))
            
        # Process chunks in parallel with concurrency limit
        semaphore = asyncio.Semaphore(config.max_concurrent_chunks)
        
        async def fetch_chunk(start: int, end: int) -> List[Dict[str, Any]]:
            async with semaphore:
                chunk_params = parameters.copy()
                chunk_params['offset'] = start
                chunk_params['limit'] = end - start
                response = await self._make_request(
                    next(iter(self.config.endpoints.keys())),
                    chunk_params
                )
                return self._extract_items(response)
                
        tasks = [fetch_chunk(start, end) for start, end in chunks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine and filter results
        all_items = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Chunk processing failed: {str(result)}")
                continue
            all_items.extend(result)
            
        return all_items

    async def _process_sequential(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Process data sequentially using standard pagination
        # This is the default processing mode when parallel processing is not enabled
        # Returns: List of processed items from all pages
        
        all_results = []
        page_count = 0
        
        # Get initial parameters from pagination strategy
        page_params = self.config.pagination.strategy.get_initial_params(
            parameters or {}, 
            self.config.pagination.page_size
        )
        
        while True:
            try:
                # Fetch and process current page
                response = await self._make_request(
                    next(iter(self.config.endpoints.keys())),
                    page_params
                )
                
                items = self._extract_items(response)
                all_results.extend(items)
                page_count += 1
                
                # Check if we should continue pagination
                if self.config.pagination.max_pages and page_count >= self.config.pagination.max_pages:
                    break
                    
                # Get next page parameters from strategy
                next_params = await self.config.pagination.strategy.get_next_page_params(
                    page_params,
                    response,
                    page_count
                )
                
                if not next_params:
                    break
                    
                page_params = next_params
                
            except Exception as e:
                logger.error(f"Sequential processing failed: {str(e)}")
                break
                
        return all_results

    async def extract_stream(self, parameters: Optional[Dict[str, Any]] = None) -> AsyncIterator[Dict[str, Any]]:
        """Stream data with built-in batching and watermark support."""
        try:
            parameters = parameters or {}
            
            if self.config.watermark and self.config.watermark.enabled:
                start_time = await self.watermark_handler.get_last_watermark()
                if not start_time:
                    start_time = (
                        self.config.watermark.initial_watermark or 
                        datetime.now(UTC) - timedelta(
                            seconds=self.config.watermark.lookback_seconds
                        )
                    )
                
                windows = self._get_window_bounds(start_time)
                logger.info(f"Processing {len(windows)} windows from {start_time}")
                
                max_watermark = start_time
                
                for window_start, window_end in windows:
                    self._metrics['window_count'] += 1
                    logger.info(f"Processing window: {window_start} to {window_end}")
                    
                    window_params = self.watermark_handler.apply_watermark_filters(
                        parameters, window_start, window_end
                    )
                    
                    items = await self._get_items_to_process(window_params)
                    if not items:
                        continue
                    
                    for i in range(0, len(items), self.config.batch_size):
                        batch = items[i:i + self.config.batch_size]
                        async for item in self._process_batch_stream(batch, window_params):
                            # Update watermark as we process
                            item_timestamp = datetime.fromisoformat(
                                str(item[self.config.watermark.timestamp_field])
                            )
                            max_watermark = max(max_watermark, item_timestamp)
                            yield item
                            
                            # Update watermark periodically
                            if max_watermark > start_time:
                                await self.watermark_handler.update_watermark(max_watermark)
                
            else:
                items = await self._get_items_to_process(parameters)
                if not items:
                    logger.warning("No items to process")
                    return
                    
                logger.info(f"Processing {len(items)} items in batches of {self.config.batch_size}")
                
                for i in range(0, len(items), self.config.batch_size):
                    batch = items[i:i + self.config.batch_size]
                    async for item in self._process_batch_stream(batch, parameters):
                        yield item
                        
        finally:
            if self.session:
                await self.session.close()
                self.session = None

    async def _process_batch_stream(self, batch: List[Dict[str, Any]], window_params: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        """Process a batch of items in parallel and stream results."""
        tasks = []
        
        async def process_item(item: Dict[str, Any]) -> Dict[str, Any]:
            """Process a single item and return the result."""
            try:
                result = await self._transform_item(item)
                self._metrics['items_processed'] += 1
                return result
            except Exception as e:
                logger.error(f"Error processing item: {e}")
                self._metrics['failed_batches'] += 1
                raise
        
        # Create tasks for each item in the batch
        for item in batch:
            tasks.append(process_item(item))
        
        # Process tasks as they complete
        for task in asyncio.as_completed(tasks):
            try:
                result = await task
                if result:
                    yield result
            except Exception as e:
                logger.error(f"Error in batch processing: {e}")
                continue


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
            'total_write_time': 0.0,
            'batch_count': 0,
            'failed_batches': 0,
            'max_batch_size': 0,
            'min_batch_size': float('inf'),
            'total_batch_size': 0
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

    async def write(self, data: List[Dict[str, Any]]) -> None:
        """Write data to the output destination."""
        start_time = time.monotonic()
        try:
            # Update batch metrics
            batch_size = len(data)
            self._metrics['batch_count'] += 1
            self._metrics['total_batch_size'] += batch_size
            self._metrics['max_batch_size'] = max(self._metrics['max_batch_size'], batch_size)
            self._metrics['min_batch_size'] = min(self._metrics['min_batch_size'], batch_size)
            
            # Update record metrics
            self._metrics['records_written'] += batch_size
            self._metrics['last_write_time'] = datetime.now(UTC)
            
            # Estimate bytes written (approximate)
            import sys
            bytes_written = sys.getsizeof(str(data))  # Approximate size
            self._metrics['bytes_written'] += bytes_written
            
        except Exception as e:
            self._metrics['write_errors'] += 1
            self._metrics['failed_batches'] += 1
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
        uptime = current_time - self._start_time
        
        # Calculate derived metrics
        metrics['uptime'] = uptime
        metrics['records_per_second'] = (
            metrics['records_written'] / uptime
            if uptime > 0 else 0
        )
        metrics['average_write_time'] = (
            metrics['total_write_time'] / metrics['batch_count']
            if metrics['batch_count'] > 0 else 0
        )
        metrics['error_rate'] = (
            metrics['write_errors'] / metrics['batch_count']
            if metrics['batch_count'] > 0 else 0
        )
        metrics['average_batch_size'] = (
            metrics['total_batch_size'] / metrics['batch_count']
            if metrics['batch_count'] > 0 else 0
        )
        metrics['bytes_per_second'] = (
            metrics['bytes_written'] / uptime
            if uptime > 0 else 0
        )
        metrics['success_rate'] = (
            (metrics['batch_count'] - metrics['failed_batches']) / metrics['batch_count']
            if metrics['batch_count'] > 0 else 0
        )
        return metrics 