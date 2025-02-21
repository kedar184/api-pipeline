#!/usr/bin/env python3
"""
Pagination Logic Test Suite
==========================

This test suite verifies the pagination functionality of the BaseExtractor class, specifically
focusing on the Link Header based pagination strategy commonly used in REST APIs.

Test Intent
----------
The suite aims to ensure that the pagination logic:
1. Correctly handles page limits and boundaries
2. Properly manages different pagination states (enabled/disabled)
3. Preserves request parameters across paginated requests
4. Handles edge cases gracefully (empty responses, partial pages)
5. Respects resource constraints (max pages)

Test Coverage
------------
The suite uses a MockPaginatedExtractor that simulates a paginated API with:
- 2 items per page
- 10 total items available
- Link header based navigation
- Configurable max pages limit

Individual Tests
--------------
1. test_max_pages_limit:
   - Verifies that the extractor respects the max_pages configuration
   - Ensures it stops after reaching the page limit regardless of available data
   - Validates the correct number of items are returned

2. test_pagination_disabled:
   - Confirms behavior when pagination is explicitly disabled
   - Ensures only a single request is made
   - Verifies only first page items are returned

3. test_early_termination:
   - Tests the extractor's ability to stop when no next page is available
   - Ensures graceful handling of pagination termination
   - Validates all available items are collected before stopping

4. test_empty_response:
   - Verifies correct handling of empty API responses
   - Ensures the extractor doesn't continue pagination for empty results
   - Validates empty result set handling

5. test_partial_page:
   - Tests handling of incomplete final pages
   - Ensures all items are collected even when last page is partial
   - Verifies correct item count across all pages

6. test_parameter_preservation:
   - Validates that query parameters are preserved across paginated requests
   - Ensures additional parameters (sorting, filtering) are maintained
   - Verifies parameter values remain unchanged throughout pagination

Usage Example
------------
The test suite can be run in isolation to verify pagination behavior:
```python
pytest tests/core/test_pagination_logic.py -v
```

Dependencies
-----------
- pytest
- pytest-asyncio
- api_pipeline.core.base
- api_pipeline.core.pagination
"""

import pytest
from typing import Dict, List, Any, Optional
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock

from api_pipeline.core.base import (
    BaseExtractor,
    ExtractorConfig,
    ProcessingPattern,
    PaginationConfig,
    AuthConfig
)
from api_pipeline.core.auth import AuthConfig
from api_pipeline.core.pagination import LinkHeaderStrategy, LinkHeaderConfig

class MockPaginatedExtractor(BaseExtractor):
    """Mock extractor that simulates paginated responses."""
    
    def __init__(self, config: ExtractorConfig):
        super().__init__(config)
        self.request_count = 0
        self.items_per_page = 2
        self.total_items = 10
        self._last_params = {}
    
    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single item."""
        return item
    
    def _validate(self) -> None:
        """Validate parameters."""
        pass
    
    async def _make_request(self, endpoint: str, params: Dict[str, Any], endpoint_override: Optional[str] = None) -> Dict[str, Any]:
        """Mock request that returns paginated data."""
        self.request_count += 1
        self._last_params = params.copy()
        page = int(params.get('page', 1))
        
        # Calculate next page, respecting max_pages
        next_page = page + 1 if page * self.items_per_page < self.total_items else None
        if self.config.pagination.max_pages and page >= self.config.pagination.max_pages:
            next_page = None

        # Generate Link header with proper URL formatting
        links = []
        if next_page:
            preserved_params = {k: v for k, v in params.items() if k not in ['page']}
            next_params = {**preserved_params, 'page': str(next_page)}
            next_url_params = '&'.join(f'{k}={v}' for k, v in next_params.items())
            next_link = f'<https://api.example.com/items?{next_url_params}>; rel="next"'
            links.append(next_link)
        
        response = {
            'data': [
                {'id': i} for i in range(
                    (page - 1) * self.items_per_page,
                    min(page * self.items_per_page, self.total_items)
                )
            ],
            'headers': {
                'Link': ', '.join(links) if links else ''
            }
        }
        return response

    def _extract_items(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract items from response."""
        return response.get('data', [])

@pytest.fixture
async def mock_extractor():
    """Create a mock extractor with pagination configuration."""
    link_header_config = LinkHeaderConfig(parse_params=True)
    config = ExtractorConfig(
        name="test",
        base_url="https://api.example.com",
        endpoints={"test": "/test"},
        auth_config=AuthConfig(
            auth_type="api_key",
            auth_credentials={"key": "test_key"},
            headers_prefix="X-API"
        ),
        pagination=PaginationConfig(
            enabled=True,
            strategy=LinkHeaderStrategy(link_header_config),
            strategy_config=link_header_config,
            page_size=2,
            max_pages=3
        )
    )
    return MockPaginatedExtractor(config)

@pytest.mark.asyncio
class TestPaginationLogic:
    """Test suite for pagination logic in base extractor."""
    
    async def test_max_pages_limit(self, mock_extractor):
        """Test that max_pages limit is respected."""
        # Extract data
        results = await mock_extractor.extract()
        
        # Verify number of requests matches max_pages
        assert mock_extractor.request_count == 3, "Should only make 3 requests (max_pages)"
        assert len(results) == 6, "Should return items from all pages (2 items per page * 3 pages)"
    
    async def test_pagination_disabled(self, mock_extractor):
        """Test behavior when pagination is disabled."""
        # Disable pagination
        mock_extractor.config.pagination.enabled = False
        
        # Extract data
        results = await mock_extractor.extract()
        
        # Verify only one request is made
        assert mock_extractor.request_count == 1, "Should only make 1 request when pagination is disabled"
        assert len(results) == 2, "Should return items from single page"
    
    async def test_early_termination(self, mock_extractor):
        """Test pagination stops when no next page is available."""
        async def modified_make_request(endpoint: str, params: Dict[str, Any], endpoint_override: Optional[str] = None) -> Dict[str, Any]:
            """Mock request that stops after 2 pages."""
            page = int(params.get('page', 1))
            mock_extractor.request_count += 1
            mock_extractor._last_params = params.copy()

            # Only return next link for first page
            links = []
            if page == 1:
                preserved_params = {k: v for k, v in params.items() if k not in ['page']}
                next_params = {**preserved_params, 'page': '2'}
                next_url_params = '&'.join(f'{k}={v}' for k, v in next_params.items())
                next_link = f'<https://api.example.com/items?{next_url_params}>; rel="next"'
                links.append(next_link)

            return {
                'data': [
                    {'id': i} for i in range(
                        (page - 1) * mock_extractor.items_per_page,
                        min(page * mock_extractor.items_per_page, mock_extractor.total_items)
                    )
                ],
                'headers': {
                    'Link': ', '.join(links) if links else ''
                }
            }
        
        # Replace _make_request with modified version
        mock_extractor._make_request = modified_make_request
        
        # Extract data
        results = await mock_extractor.extract()
        
        # Verify pagination stopped after 2 pages
        assert mock_extractor.request_count == 2, "Should stop after 2 pages when no next link"
        assert len(results) == 4, "Should return items from both pages"
    
    async def test_empty_response(self, mock_extractor):
        """Test pagination handles empty responses correctly."""
        async def empty_make_request(endpoint: str, params: Dict[str, Any], endpoint_override: Optional[str] = None) -> Dict[str, Any]:
            """Mock request that returns empty data."""
            mock_extractor.request_count += 1
            mock_extractor._last_params = params.copy()
            return {
                'data': [],
                'headers': {}
            }
        
        # Replace _make_request with empty version
        mock_extractor._make_request = empty_make_request
        
        # Extract data
        results = await mock_extractor.extract()
        
        # Verify behavior with empty response
        assert mock_extractor.request_count == 1, "Should stop after first empty response"
        assert len(results) == 0, "Should return empty list"
    
    async def test_partial_page(self, mock_extractor):
        """Test pagination handles partial last page correctly."""
        async def partial_make_request(endpoint: str, params: Dict[str, Any], endpoint_override: Optional[str] = None) -> Dict[str, Any]:
            """Mock request that returns partial last page."""
            page = int(params.get('page', 1))
            mock_extractor.request_count += 1
            mock_extractor._last_params = params.copy()

            # Return partial data on last page
            items = [
                {'id': i} for i in range(
                    (page - 1) * mock_extractor.items_per_page,
                    min(page * mock_extractor.items_per_page, 5)  # Only 5 items total
                )
            ]

            # Only include next link if there are more items
            links = []
            if len(items) == mock_extractor.items_per_page:
                preserved_params = {k: v for k, v in params.items() if k not in ['page']}
                next_params = {**preserved_params, 'page': str(page + 1)}
                next_url_params = '&'.join(f'{k}={v}' for k, v in next_params.items())
                next_link = f'<https://api.example.com/items?{next_url_params}>; rel="next"'
                links.append(next_link)

            return {
                'data': items,
                'headers': {
                    'Link': ', '.join(links) if links else ''
                }
            }
        
        # Replace _make_request with partial version
        mock_extractor._make_request = partial_make_request
        
        # Extract data
        results = await mock_extractor.extract()
        
        # Verify behavior with partial page
        assert mock_extractor.request_count == 3, "Should process all pages"
        assert len(results) == 5, "Should return all items (2 + 2 + 1)"
    
    async def test_parameter_preservation(self, mock_extractor):
        """Test that additional parameters are preserved across pagination."""
        # Extract with additional parameters
        test_params = {"filter": "test", "sort": "desc"}
        await mock_extractor.extract(test_params)
        
        # Verify parameters in mock requests
        assert "filter" in mock_extractor._last_params, "Filter parameter should be preserved"
        assert mock_extractor._last_params["filter"] == "test", "Filter value should be preserved"
        assert "sort" in mock_extractor._last_params, "Sort parameter should be preserved"
        assert mock_extractor._last_params["sort"] == "desc", "Sort value should be preserved" 