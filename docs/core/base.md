# Base Classes

The base module defines the foundational abstract classes for the pipeline system. These classes establish the core interfaces for data extraction and output handling.

## Core Components

### Pagination System

#### PaginationType (Enum)
Defines supported pagination strategies:
```python
class PaginationType(str, Enum):
    PAGE_NUMBER = "page_number"  # ?page=1&per_page=100
    CURSOR = "cursor"           # ?cursor=abc123
    OFFSET = "offset"           # ?offset=100&limit=50
    TOKEN = "token"             # ?page_token=xyz789
    LINK = "link"               # Uses Link headers (GitHub style)
```

#### PaginationConfig
Configuration model for API pagination with strategy-specific settings:

```python
class PaginationConfig(BaseModel):
    enabled: bool = True
    strategy: PaginationType = PaginationType.PAGE_NUMBER
    page_size: int = 100
    max_pages: Optional[int] = None
    
    # Strategy-specific parameters
    page_param: str = "page"              # For PAGE_NUMBER
    cursor_param: str = "cursor"          # For CURSOR
    offset_param: str = "offset"          # For OFFSET
    token_param: str = "page_token"       # For TOKEN
    
    # Response parsing
    items_field: str = "items"
    total_field: Optional[str] = None
    has_more_field: Optional[str] = None
```

Example configurations for different strategies:

1. **Page Number** (Default)
```yaml
pagination:
  strategy: page_number
  page_param: "page"
  size_param: "per_page"
  page_size: 50
```

2. **Cursor-based**
```yaml
pagination:
  strategy: cursor
  cursor_param: "after"
  cursor_field: "pageInfo.endCursor"
  has_more_field: "pageInfo.hasNextPage"
```

3. **Offset-based**
```yaml
pagination:
  strategy: offset
  offset_param: "offset"
  limit_param: "limit"
  total_field: "total_count"
```

4. **Token-based**
```yaml
pagination:
  strategy: token
  token_param: "page_token"
  next_token_field: "nextPageToken"
```

5. **Link Header**
```yaml
pagination:
  strategy: link
  page_size: 30
```

### ExtractorConfig

Configuration model for extractors. Key attributes:

```python
class ExtractorConfig(BaseModel):
    base_url: str
    endpoints: Dict[str, str]
    auth_config: AuthConfig  # Authentication configuration
    rate_limit: Optional[int] = None
    retry_count: Optional[int] = None
    batch_size: int = 100
    max_concurrent_requests: int = 10
    session_timeout: int = 30
    pagination: Optional[PaginationConfig] = None
```

#### Authentication Integration
The `auth_config` field integrates with the authentication system:
```python
auth_config = AuthConfig(
    auth_type="oauth",
    auth_credentials={...}
)
```

The extractor automatically:
1. Initializes appropriate auth handler
2. Manages token refresh
3. Adds auth headers to requests
4. Handles auth errors

#### Watermark-Based Extraction

The extractor supports watermark-based incremental extraction with configurable time windows. This feature enables efficient processing of time-series data by tracking the last processed timestamp and processing data in configurable windows.

##### Configuration

1. **Window Configuration**
```python
window_config = WindowConfig(
    window_type=WindowType.FIXED,  # FIXED, SLIDING, or SESSION
    window_size="1h",             # Duration string (e.g., "1h", "1d")
    window_offset="0m",           # Offset for window start
    window_overlap="0m",          # For sliding windows
    timestamp_field="timestamp"   # Field for windowing
)
```

2. **Watermark Configuration**
```python
watermark_config = WatermarkConfig(
    enabled=True,
    timestamp_field="updated_at",      # Field to track
    watermark_field="last_watermark",  # Storage field
    window=window_config,
    initial_watermark=datetime.now(UTC) - timedelta(days=7),
    lookback_window="1h"               # How far to look back
)
```

##### Implementation

1. **Window Management**
```python
def _get_window_bounds(self, start_time: datetime) -> List[Tuple[datetime, datetime]]:
    """Calculate time windows for extraction."""
    window_size = self.config.watermark.window.window_size_seconds
    current_time = datetime.now(UTC)
    
    windows = []
    window_start = start_time
    while window_start < current_time:
        window_end = min(
            window_start + timedelta(seconds=window_size),
            current_time
        )
        windows.append((window_start, window_end))
        window_start = window_end
    
    return windows
```

2. **Watermark Tracking**
```python
async def _update_watermark(self, new_watermark: datetime) -> None:
    """Update the watermark value."""
    self._current_watermark = new_watermark
    self._metrics['watermark_updates'] += 1
```

##### Example Usage

```python
# Configure the extractor
config = ExtractorConfig(
    base_url="https://api.example.com",
    endpoints={"data": "/data"},
    watermark=WatermarkConfig(
        enabled=True,
        timestamp_field="updated_at",
        window=WindowConfig(
            window_type=WindowType.FIXED,
            window_size="1h"
        )
    )
)

# The extractor will:
# 1. Track the last processed timestamp
# 2. Process data in hourly windows
# 3. Apply timestamp filters automatically
# 4. Update watermark after successful processing
```

##### Benefits

1. **Efficient Processing**
   - Process only new or updated data
   - Prevent duplicate processing
   - Optimize API usage

2. **Flexible Configuration**
   - Multiple window types (FIXED, SLIDING, SESSION)
   - Configurable window sizes
   - Customizable timestamp fields

3. **Robust Error Handling**
   - Automatic retry on failure
   - Watermark updates only after successful processing
   - Progress tracking and monitoring

4. **Performance Optimization**
   - Parallel window processing
   - Controlled data flow
   - Resource-efficient extraction

### BaseExtractor

Abstract base class with built-in performance optimizations. Key features:
- Concurrent request processing
- Automatic batching
- Connection pooling
- Rate limiting
- Resource management
- Integrated authentication

#### Authentication Integration
The `_ensure_session` method handles authentication setup:
```python
async def _ensure_session(self):
    """Initialize session with authentication."""
    if not self.session:
        self.auth_handler = create_auth_handler(self.config.auth_config)
        headers = await self.auth_handler.get_auth_headers()
        self.session = aiohttp.ClientSession(headers=headers)
```

#### Performance Optimizations

1. **Parallel Processing**
   ```python
   async def _process_batch(self, batch: List[Any]) -> List[Dict[str, Any]]:
       """Process items concurrently within rate limits."""
       semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
       tasks = []
       
       async def process_item(item: Any) -> Dict[str, Any]:
           async with semaphore:  # Control concurrency
               params = self._get_item_params(item)
               response = await self._make_request("items", params)
               return await self._transform_item(response)
       
       # Create tasks for all items
       tasks = [process_item(item) for item in batch]
       # Execute all tasks concurrently
       return await asyncio.gather(*tasks)
   ```
   - Uses `asyncio.gather` for concurrent execution
   - Controls concurrency with semaphores
   - Respects rate limits
   - Automatically batches requests

2. **Connection Pooling**
   ```python
   # Session is reused across requests
   self.session = aiohttp.ClientSession(
       headers=headers,
       timeout=timeout,
       connector=aiohttp.TCPConnector(
           limit=self.config.max_concurrent_requests
       )
   )
   ```
   - Reuses HTTP connections
   - Reduces connection overhead
   - Manages connection lifecycle
   - Configurable connection limits

3. **Batch Processing**
   ```python
   async def extract(self, parameters: Optional[Dict] = None) -> List[Dict]:
       batches = self._get_batches(parameters)  # Split into batches
       results = []
       
       for batch in batches:
           # Process each batch concurrently
           batch_results = await self._process_batch(batch)
           results.extend(batch_results)
   ```
   - Splits large datasets into manageable batches
   - Processes batches concurrently
   - Optimizes memory usage
   - Prevents overwhelming APIs

4. **Rate Limiting**
   ```python
   async def _make_request(self, endpoint: str, params: Dict) -> Dict:
       await self._rate_limiter.acquire()  # Throttle requests
       try:
           return await self._do_request(endpoint, params)
       finally:
           self._rate_limiter.release()
   ```
   - Implements token bucket algorithm
   - Prevents API rate limit violations
   - Configurable rate limits
   - Automatic request throttling

5. **Retry Mechanism with Backoff**
   ```python
   @backoff.on_exception(
       backoff.expo,
       (aiohttp.ClientError, TimeoutError),
       max_tries=3,
       max_time=30,
       on_backoff=lambda details: logger.warning(f"Retrying request: {details}")
   )
   async def _do_request(self, endpoint: str, params: Dict) -> Dict:
       """Make HTTP request with exponential backoff retry."""
       try:
           async with self.session.get(
               f"{self.config.base_url}{self.config.endpoints[endpoint]}",
               params=params
           ) as response:
               response.raise_for_status()
               return await response.json()
       except aiohttp.ClientResponseError as e:
           if e.status in (429, 503):  # Rate limit or service unavailable
               retry_after = int(response.headers.get('Retry-After', 5))
               logger.warning(f"Rate limited. Waiting {retry_after} seconds")
               await asyncio.sleep(retry_after)
               raise  # Will be retried by backoff decorator
           elif e.status >= 400:  # Other client/server errors
               logger.error(f"Request failed: {str(e)}")
               raise
   ```

   Configuration options:
   ```yaml
   api_config:
     retry:
       max_attempts: 3          # Maximum number of retry attempts
       max_time: 30            # Maximum total retry time in seconds
       base_delay: 1           # Initial delay between retries
       max_delay: 10          # Maximum delay between retries
       jitter: true           # Add randomness to delay
       retry_on:              # HTTP status codes to retry
         - 429               # Too Many Requests
         - 503               # Service Unavailable
         - 504               # Gateway Timeout
   ```

   Features:
   - Exponential backoff with jitter
   - Configurable retry attempts and delays
   - Smart handling of rate limits
   - Respect for Retry-After headers
   - Detailed logging of retry attempts
   - Different strategies for different errors

   Retry Scenarios:
   1. **Transient Network Issues**
      ```python
      # Automatically retries on network errors
      except aiohttp.ClientConnectorError:
          logger.warning("Network error, retrying...")
          raise  # Will be retried
      ```

   2. **Rate Limiting**
      ```python
      # Respects API rate limits
      if response.status == 429:
          retry_after = int(response.headers.get('Retry-After', 5))
          await asyncio.sleep(retry_after)
          raise  # Will be retried
      ```

   3. **Service Unavailability**
      ```python
      # Handles temporary service issues
      if response.status in (502, 503, 504):
          logger.warning("Service temporarily unavailable")
          raise  # Will be retried with backoff
      ```

   Implementation Details:
   1. **Base Delay Calculation**
      ```python
      def calculate_delay(attempt: int, base_delay: float = 1.0) -> float:
          """Calculate exponential backoff delay with jitter."""
          delay = min(base_delay * (2 ** attempt), self.config.retry.max_delay)
          return delay * (1 + random.random())  # Add jitter
      ```

   2. **Status Code Handling**
      ```python
      def should_retry(status_code: int) -> bool:
          """Determine if request should be retried based on status."""
          return (
              status_code in self.config.retry.retry_on or
              status_code >= 500
          )
      ```

   3. **Retry State Management**
      ```python
      class RetryState:
          def __init__(self):
              self.attempts = 0
              self.start_time = time.monotonic()
              self.last_error = None
              
          def should_continue(self, config: RetryConfig) -> bool:
              return (
                  self.attempts < config.max_attempts and
                  time.monotonic() - self.start_time < config.max_time
              )
      ```

   Best Practices:
   1. **Retry Configuration**
      - Set appropriate max attempts
      - Use reasonable timeouts
      - Configure status codes to retry
      - Enable jitter for distributed systems

   2. **Error Classification**
      - Retry transient errors
      - Fail fast on permanent errors
      - Handle rate limits specially
      - Log retry attempts appropriately

   3. **Resource Management**
      - Close sessions after max retries
      - Release rate limit tokens
      - Clean up any open connections
      - Monitor retry metrics

6. **Resource Management**
   ```python
   async def __aenter__(self):
       await self._ensure_session()
       return self

   async def __aexit__(self, exc_type, exc, tb):
       if self.session:
           await self.session.close()
   ```
   - Proper resource cleanup
   - Context manager support
   - Automatic session management
   - Memory leak prevention

#### Performance Tuning

Configure these parameters for optimal performance:

```yaml
api_config:
  # Concurrency settings
  max_concurrent_requests: 10  # Number of parallel requests
  batch_size: 100             # Items per batch
  
  # Timeouts and limits
  session_timeout: 30         # Request timeout in seconds
  rate_limit: 100            # Requests per second
  
  # Connection pooling
  connection_limit: 100      # Max concurrent connections
  connection_timeout: 5      # Connection timeout
```

Performance Impact Examples:

1. **Sequential vs Parallel**
   - Sequential: 100 requests * 200ms = 20 seconds
   - Parallel (10 concurrent): 100 requests * 200ms / 10 = 2 seconds

2. **Batch Processing**
   - Individual: 1000 items = 1000 requests
   - Batched (100): 1000 items = 10 requests

3. **Connection Pooling**
   - New connections: +100ms per request
   - Pooled: +10ms per request

#### Best Practices for Performance

1. **Batch Size Selection**
   - Consider API limits
   - Memory constraints
   - Network latency
   - Response size

2. **Concurrency Levels**
   - API rate limits
   - System resources
   - Network capacity
   - Response times

3. **Error Handling**
   - Retry strategies
   - Backoff policies
   - Partial success handling
   - Resource cleanup

4. **Monitoring**
   - Request latency
   - Success rates
   - Resource usage
   - Error patterns

#### Required Methods to Implement

1. **_transform_item**
```python
@abstractmethod
async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a single item of data."""
    pass
```

The `_transform_item` method is the core transformation method that every extractor must implement. It's responsible for converting raw API response data into a standardized format for downstream processing.

**Key Aspects:**
- **Asynchronous**: Defined as `async` to support concurrent processing
- **Single Item**: Processes one data item at a time
- **Type Safety**: Takes and returns dictionaries with flexible value types
- **Stateless**: Should not maintain state between transformations

**Parameters:**
- `item` (Dict[str, Any]): A single item from the API response
  - Can contain nested structures
  - May include raw API-specific fields
  - Types can vary (strings, numbers, booleans, lists, objects)

**Returns:**
- Dict[str, Any]: Transformed data in standardized format
  - Should match output schema
  - Must be JSON-serializable
  - Should include all required fields

**Common Transformations:**
1. **Field Mapping**
   ```python
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       return {
           "user_id": item["id"],
           "full_name": f"{item['first_name']} {item['last_name']}",
           "email": item["email_address"].lower()
       }
   ```

2. **Data Type Conversion**
   ```python
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       return {
           "timestamp": datetime.fromisoformat(item["created_at"]).isoformat(),
           "temperature": float(item["temp"]),
           "is_active": bool(item["status"])
       }
   ```

3. **Nested Data Handling**
   ```python
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       return {
           "order_id": item["id"],
           "customer_name": item["customer"]["name"],
           "total_amount": sum(line["price"] for line in item["line_items"]),
           "item_count": len(item["line_items"])
       }
   ```

4. **Data Enrichment**
   ```python
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       return {
           "id": item["id"],
           "status": item["status"],
           "processed_at": datetime.now(UTC).isoformat(),
           "source_system": self.config.system_name,
           "environment": os.getenv("ENVIRONMENT", "dev")
       }
   ```

**Best Practices:**

1. **Error Handling**
   ```python
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       try:
           return {
               "id": item["id"],
               "name": item.get("name", "Unknown"),  # Default value
               "score": float(item["score"]) if item.get("score") else 0.0
           }
       except (KeyError, ValueError) as e:
           logger.error(f"Error transforming item {item.get('id', 'unknown')}: {str(e)}")
           raise
   ```

2. **Data Validation**
   ```python
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       if not item.get("id"):
           raise ValueError("Item must have an ID")
           
       return {
           "id": str(item["id"]),  # Ensure string type
           "amount": max(0, float(item["amount"])),  # Ensure non-negative
           "status": item["status"].lower()  # Normalize case
       }
   ```

3. **Complex Transformations**
   ```python
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       def calculate_total(items):
           return sum(item["quantity"] * item["price"] for item in items)
           
       return {
           "order_id": item["id"],
           "items_total": calculate_total(item["items"]),
           "tax": self._calculate_tax(item["items_total"]),
           "shipping": self._get_shipping_cost(item["address"]),
           "grand_total": sum([
               calculate_total(item["items"]),
               self._calculate_tax(item["items_total"]),
               self._get_shipping_cost(item["address"])
           ])
       }
   ```

**Common Pitfalls to Avoid:**

1. **Modifying Input**
   ```python
   # BAD: Modifying input
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       item["processed"] = True  # Don't modify input
       return item
       
   # GOOD: Create new dictionary
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       return {**item, "processed": True}
   ```

2. **External Service Calls**
   ```python
   # BAD: Making external calls
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       details = await self._api.get_details(item["id"])  # Avoid API calls here
       
   # GOOD: Use pre-fetched data
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       details = item["details"]  # Use data already fetched
   ```

3. **Heavy Computations**
   ```python
   # BAD: Complex processing
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       result = await self._heavy_computation(item)  # Avoid heavy processing
       
   # GOOD: Simple transformations
   async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
       return {
           "id": item["id"],
           "total": sum(item["values"])  # Keep it simple
       }
   ```

**Testing Considerations:**

1. **Unit Testing**
   ```python
   def test_transform_item():
       extractor = CustomExtractor(config)
       input_item = {"id": 1, "name": "Test"}
       result = await extractor._transform_item(input_item)
       assert result["id"] == "1"  # Test type conversion
       assert "timestamp" in result  # Test added fields
   ```

2. **Edge Cases**
   ```python
   def test_transform_item_edge_cases():
       extractor = CustomExtractor(config)
       # Test missing fields
       assert await extractor._transform_item({}) == {"id": None, "name": "Unknown"}
       # Test invalid data
       with pytest.raises(ValueError):
           await extractor._transform_item({"id": "invalid"})
   ```

#### Optional Methods to Override

1. **_get_item_params** - Customize request parameters
```python
def _get_item_params(self, item: Any) -> Dict[str, Any]:
    return {"id": item} if isinstance(item, (str, int)) else item
```

2. **_ensure_session** - Configure API session
```python
async def _ensure_session(self):
    if not self.session:
        timeout = aiohttp.ClientTimeout(total=self.config.session_timeout)
        self.session = aiohttp.ClientSession(timeout=timeout)
```

#### Core Methods (Do Not Override)

1. **extract** - Main extraction method
   - Handles batching and parallel processing
   - Manages resource lifecycle
   - Implements error handling

2. **_paginated_request** - Pagination handler
   - Supports multiple pagination strategies
   - Handles response parsing
   - Manages continuation logic

3. **_process_batch** - Batch processor
   - Implements concurrent processing
   - Handles error recovery
   - Aggregates results

### BaseOutput

Abstract base class for output handlers:

```python
class BaseOutput(ABC):
    @abstractmethod
    async def write(self, data: List[Dict[str, Any]]) -> None:
        pass
    
    @abstractmethod
    async def close(self) -> None:
        pass
```

## Implementation Guide

### Creating a New Extractor

1. **Basic Implementation**
```python
class CustomExtractor(BaseExtractor):
    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": item["id"],
            "name": item["name"],
            "timestamp": datetime.now(UTC)
        }
```

2. **With Custom Session**
```python
class CustomExtractor(BaseExtractor):
    async def _ensure_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.config.auth_credentials['token']}"},
                timeout=aiohttp.ClientTimeout(total=self.config.session_timeout)
            )
```

3. **With Custom Parameters**
```python
class CustomExtractor(BaseExtractor):
    def _get_item_params(self, item: Any) -> Dict[str, Any]:
        return {
            "query": item.get("search_term"),
            "filter": item.get("category")
        }
```

## Performance Considerations

1. **Batch Size**
   - Default: 100 items per batch
   - Adjust based on API limits and item size
   - Configure via `batch_size` in config

2. **Concurrency**
   - Default: 10 concurrent requests
   - Adjust based on API rate limits
   - Configure via `max_concurrent_requests`

3. **Timeouts**
   - Default: 30 seconds
   - Adjust for slow APIs
   - Configure via `session_timeout`

4. **Pagination**
   - Default page size: 100
   - Adjust based on API recommendations
   - Configure via `page_size` in pagination config

## Error Handling

The base implementation provides several layers of error handling:

1. **Request Level**
   - Retries for transient failures
   - Rate limit handling
   - Timeout management

2. **Batch Level**
   - Continues processing on item failures
   - Logs individual item errors
   - Aggregates successful results

3. **Resource Management**
   - Automatic session cleanup
   - Semaphore for concurrency control
   - Connection pooling

## Best Practices

1. **Configuration**
   - Use appropriate pagination strategy
   - Set reasonable batch sizes
   - Configure timeouts appropriately
   - Enable rate limiting if needed

2. **Implementation**
   - Keep transformations simple
   - Handle item-specific errors
   - Clean up resources properly
   - Log meaningful messages

3. **Testing**
   - Test with different batch sizes
   - Verify pagination handling
   - Check error scenarios
   - Validate resource cleanup

## Extension Points

1. **New Extractors**
   - API integrations
   - Database extractors
   - File system readers

2. **New Outputs**
   - Database writers
   - Message queues
   - File formats

3. **Custom Configurations**
   - Authentication methods
   - Connection pooling
   - Retry strategies 