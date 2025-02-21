import pytest
from typing import Dict, Any, Optional
from datetime import datetime, UTC
from urllib.parse import urlparse, parse_qs

from api_pipeline.core.pagination import (
    PaginationStrategy,
    PageNumberStrategy,
    CursorStrategy,
    LinkHeaderStrategy,
    OffsetStrategy,
    PaginationConfig,
    PageNumberConfig,
    CursorConfig,
    LinkHeaderConfig,
    OffsetConfig
)

# Test Data
MOCK_RESPONSE_WITH_ITEMS = {
    "data": [{"id": 1}, {"id": 2}],
    "headers": {
        "Link": '<https://api.test.com/items?page=2>; rel="next"'
    }
}

MOCK_RESPONSE_WITH_CURSOR = {
    "data": [{"id": 1}, {"id": 2}],
    "next_cursor": "next_page_token"
}

MOCK_RESPONSE_WITH_OFFSET = {
    "data": [{"id": 1}, {"id": 2}],
    "total_count": 100
}

class TestPageNumberStrategy:
    """Test suite for page number based pagination."""
    
    @pytest.fixture
    def strategy(self) -> PageNumberStrategy:
        return PageNumberStrategy(PageNumberConfig())
    
    async def test_initial_params(self, strategy):
        """Test initial parameters generation."""
        base_params = {"query": "test"}
        result = strategy.get_initial_params(base_params, 10)
        
        assert result["page"] == 1
        assert result["per_page"] == 10
        assert result["query"] == "test"
    
    async def test_next_page_params_with_more_items(self, strategy):
        """Test next page parameters when more items exist."""
        current_params = {"page": 1, "per_page": 2}
        result = await strategy.get_next_page_params(
            current_params,
            [{"id": 1}, {"id": 2}],  # Full page of items
            1
        )
        
        assert result is not None
        assert result["page"] == 2
        assert result["per_page"] == 2
    
    async def test_next_page_params_no_more_items(self, strategy):
        """Test next page parameters when no more items exist."""
        current_params = {"page": 1, "per_page": 2}
        result = await strategy.get_next_page_params(
            current_params,
            [{"id": 1}],  # Partial page
            1
        )
        
        assert result is None

class TestCursorStrategy:
    """Test suite for cursor based pagination."""
    
    @pytest.fixture
    def strategy(self) -> CursorStrategy:
        return CursorStrategy(CursorConfig())
    
    async def test_initial_params(self, strategy):
        """Test initial parameters generation."""
        base_params = {"query": "test"}
        result = strategy.get_initial_params(base_params, 10)
        
        assert result == base_params
    
    async def test_next_page_params_with_cursor(self, strategy):
        """Test next page parameters when cursor exists."""
        current_params = {"query": "test"}
        result = await strategy.get_next_page_params(
            current_params,
            MOCK_RESPONSE_WITH_CURSOR,
            1
        )
        
        assert result is not None
        assert result["cursor"] == "next_page_token"
        assert result["query"] == "test"
    
    async def test_next_page_params_no_cursor(self, strategy):
        """Test next page parameters when no cursor exists."""
        current_params = {"query": "test"}
        result = await strategy.get_next_page_params(
            current_params,
            {"data": [{"id": 1}]},
            1
        )
        
        assert result is None

class TestLinkHeaderStrategy:
    """Test suite for Link header based pagination."""
    
    @pytest.fixture
    def strategy(self) -> LinkHeaderStrategy:
        return LinkHeaderStrategy(LinkHeaderConfig())
    
    async def test_initial_params(self, strategy):
        """Test initial parameters generation."""
        base_params = {"query": "test"}
        result = strategy.get_initial_params(base_params, 10)
        
        assert result["query"] == "test"
        assert result["per_page"] == 10
    
    async def test_next_page_params_with_link(self, strategy):
        """Test next page parameters when Link header exists."""
        current_params = {"page": "1", "per_page": "2"}
        result = await strategy.get_next_page_params(
            current_params,
            MOCK_RESPONSE_WITH_ITEMS,
            1
        )
        
        assert result is not None
        assert "page" in result
        assert result["page"] == "2"
    
    async def test_next_page_params_no_link(self, strategy):
        """Test next page parameters when no Link header exists."""
        current_params = {"page": "1", "per_page": "2"}
        result = await strategy.get_next_page_params(
            current_params,
            {"data": [{"id": 1}], "headers": {}},
            1
        )
        
        assert result is None
    
    async def test_preserve_essential_params(self, strategy):
        """Test preservation of essential parameters."""
        current_params = {
            "page": "1",
            "per_page": "2",
            "repo": "test/repo",
            "since": "2024-01-01"
        }
        result = await strategy.get_next_page_params(
            current_params,
            MOCK_RESPONSE_WITH_ITEMS,
            1
        )
        
        assert result is not None
        assert result["repo"] == "test/repo"
        assert result["since"] == "2024-01-01"

class TestOffsetStrategy:
    """Test suite for offset based pagination."""
    
    @pytest.fixture
    def strategy(self) -> OffsetStrategy:
        return OffsetStrategy(OffsetConfig())
    
    async def test_initial_params(self, strategy):
        """Test initial parameters generation."""
        base_params = {"query": "test"}
        result = strategy.get_initial_params(base_params, 10)
        
        assert result["offset"] == 0
        assert result["limit"] == 10
        assert result["query"] == "test"
    
    async def test_next_page_params_with_more_items(self, strategy):
        """Test next page parameters when more items exist."""
        current_params = {"offset": 0, "limit": 2}
        result = await strategy.get_next_page_params(
            current_params,
            [{"id": 1}, {"id": 2}],  # Full page
            1
        )
        
        assert result is not None
        assert result["offset"] == 2
        assert result["limit"] == 2
    
    async def test_next_page_params_no_more_items(self, strategy):
        """Test next page parameters when no more items exist."""
        current_params = {"offset": 0, "limit": 2}
        result = await strategy.get_next_page_params(
            current_params,
            [{"id": 1}],  # Partial page
            1
        )
        
        assert result is None

class TestPaginationConfig:
    """Test suite for pagination configuration."""
    
    def test_with_page_numbers(self):
        """Test page number pagination configuration."""
        config = PaginationConfig.with_page_numbers(
            page_size=20,
            page_param="p",
            size_param="size"
        )
        
        assert config.enabled
        assert isinstance(config.strategy, PageNumberStrategy)
        assert config.page_size == 20
        assert config.strategy_config.page_param == "p"
        assert config.strategy_config.size_param == "size"
    
    def test_with_cursor(self):
        """Test cursor pagination configuration."""
        config = PaginationConfig.with_cursor(
            page_size=20,
            cursor_param="next_token",
            cursor_field="nextPageToken"
        )
        
        assert config.enabled
        assert isinstance(config.strategy, CursorStrategy)
        assert config.page_size == 20
        assert config.strategy_config.cursor_param == "next_token"
        assert config.strategy_config.cursor_field == "nextPageToken"
    
    def test_with_offset(self):
        """Test offset pagination configuration."""
        config = PaginationConfig.with_offset(
            page_size=20,
            offset_param="start",
            limit_param="count"
        )
        
        assert config.enabled
        assert isinstance(config.strategy, OffsetStrategy)
        assert config.page_size == 20
        assert config.strategy_config.offset_param == "start"
        assert config.strategy_config.limit_param == "count"
    
    def test_with_link_header(self):
        """Test Link header pagination configuration."""
        config = PaginationConfig.with_link_header(
            page_size=20,
            rel_next="next",
            parse_params=True
        )
        
        assert config.enabled
        assert isinstance(config.strategy, LinkHeaderStrategy)
        assert config.page_size == 20
        assert config.strategy_config.rel_next == "next"
        assert config.strategy_config.parse_params 