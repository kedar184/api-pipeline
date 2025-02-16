# Building an Extractor with API Pipeline

This document provides a guide for implementing a new extractor using the API Pipeline framework. It covers the key components, patterns, and best practices for building efficient and maintainable extractors.

## Overview

An extractor is responsible for:
1. Fetching data from an API source
2. Transforming the data into a standardized format
3. Supporting incremental loading with watermarks
4. Handling pagination and rate limiting
5. Managing authentication and retries

## Implementation Steps

### 1. Create the Extractor Class

Start by creating a new class that inherits from `BaseExtractor`:

```python
from api_pipeline.core.base import BaseExtractor, ExtractorConfig

class MyApiExtractor(BaseExtractor):
    """Fetches data from MyAPI service."""
    
    def __init__(self, config: ExtractorConfig):
        super().__init__(config)
        self._watermark_store = {}  # For demo, use proper storage in production
```

### 2. Configure Authentication

Override `_ensure_session` to set up API-specific authentication:

```python
async def _ensure_session(self):
    """Initialize session with API-specific headers."""
    await super()._ensure_session(
        additional_headers={
            "Accept": "application/json",
            "X-API-Version": "v2"
        }
    )
```

### 3. Implement Parameter Validation

Add validation for extractor-specific parameters:

```python
def validate_parameters(self, parameters: Optional[Dict[str, Any]] = None) -> None:
    """Validate the input parameters."""
    if not parameters or "account_id" not in parameters:
        raise ValueError("'account_id' parameter is required")
    
    # Validate parameter format
    account_id = parameters["account_id"]
    if not re.match(r"^[A-Z0-9]{8}$", account_id):
        raise ValueError("Invalid account_id format")
```

### 4. Implement Lightweight Transforms

Create transform functions for each endpoint type:

```python
async def _transform_item(self, item: Dict[str, Any], endpoint: str) -> Dict[str, Any]:
    """Transform a single API response item based on endpoint type."""
    transforms = {
        'users': self._transform_user,
        'orders': self._transform_order
    }
    
    if endpoint not in transforms:
        raise ValueError(f"Unsupported endpoint type: {endpoint}")
        
    return await transforms[endpoint](item)

async def _transform_user(self, item: Dict[str, Any]) -> Dict[str, Any]:
    """Transform user data - keep it simple!"""
    return {
        "user_id": item["id"],
        "name": item["display_name"],
        "email": item["email"],
        "created_at": item["creation_date"]
    }
```

### 5. Configure Watermark-Based Loading

Set up watermark configuration in your YAML:

```yaml
watermark:
  enabled: true
  timestamp_field: "created_at"
  watermark_field: "last_processed"
  window:
    window_size: "1h"
    timestamp_field: "created_at"
  lookback_window: "7d"
```

Override watermark filters if needed:

```python
def _apply_watermark_filters(
    self,
    params: Dict[str, Any],
    window_start: datetime,
    window_end: datetime
) -> Dict[str, Any]:
    """Apply API-specific timestamp filters."""
    params = params or {}
    params['created_after'] = window_start.isoformat()
    params['created_before'] = window_end.isoformat()
    return params
```

### 6. Implement the Extract Method

Tie everything together in the extract method:

```python
async def extract(self, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Extract data from the API."""
    try:
        await self._ensure_session()
        parameters = parameters or {}
        
        # Configure endpoint
        account_id = parameters["account_id"]
        endpoint_override = f"/accounts/{account_id}/users"
        
        # Use base class extraction with endpoint override
        return await self._extract_base(
            parameters,
            endpoint_override=endpoint_override
        )
    finally:
        if self.session:
            await self.session.close()
            self.session = None
```

## Configuration Example

Create a YAML configuration for your extractor:

```yaml
pipeline_id: my_api_pipeline
description: "Extracts user data from MyAPI"
enabled: true
extractor_class: "my_package.extractors.MyApiExtractor"

api_config:
  base_url: "https://api.example.com/v2"
  endpoints:
    users: "/accounts/{account_id}/users"
    orders: "/accounts/{account_id}/orders"
  auth_type: "bearer"
  auth_credentials:
    token: "${secret:projects/my-project/secrets/api-token}"
  
  # Performance settings
  rate_limit: 100
  retry_count: 3
  batch_size: 50
  max_concurrent_requests: 10
  
  # Pagination settings
  pagination:
    enabled: true
    strategy: "page_number"
    page_size: 100
    max_pages: 10
    
  # Watermark settings
  watermark:
    enabled: true
    timestamp_field: "created_at"
    window:
      window_size: "1h"
    lookback_window: "7d"

parameters:
  - name: "account_id"
    type: "string"
    required: true
    description: "Account ID to fetch data for"

output:
  - type: "bigquery"
    enabled: true
    config:
      dataset_id: "raw_data"
      table_id: "users"
      schema:
        - name: "user_id"
          type: "STRING"
        - name: "name"
          type: "STRING"
        - name: "email"
          type: "STRING"
        - name: "created_at"
          type: "TIMESTAMP"
```

## Best Practices

1. **Error Handling**
   - Implement proper cleanup in finally blocks
   - Use descriptive error messages
   - Log relevant context for debugging

2. **Performance**
   - Use appropriate batch sizes
   - Configure concurrent requests based on API limits
   - Implement proper rate limiting

3. **Testing**
   - Write unit tests for transforms
   - Test parameter validation
   - Mock API responses for testing
   - Test error scenarios

4. **Monitoring**
   - Use the built-in metrics
   - Log important events
   - Track API response times

## Common Pitfalls

1. **Heavy Transforms**
   - Keep transforms lightweight
   - Use proper ETL tools for complex transformations
   - Focus on field mapping and standardization

2. **Resource Management**
   - Always close sessions
   - Clean up resources properly
   - Handle timeouts appropriately

3. **Configuration**
   - Don't hardcode credentials
   - Use appropriate rate limits
   - Configure reasonable batch sizes

## Example Test Implementation

```python
async def test_my_api_extractor():
    config = ExtractorConfig(
        base_url="https://api.example.com",
        endpoints={"users": "/users"},
        auth_config={"type": "bearer", "token": "test"}
    )
    
    extractor = MyApiExtractor(config)
    
    # Test parameter validation
    with pytest.raises(ValueError):
        await extractor.extract({})  # Missing account_id
    
    # Test successful extraction
    result = await extractor.extract({"account_id": "12345678"})
    assert len(result) > 0
    assert "user_id" in result[0]
``` 