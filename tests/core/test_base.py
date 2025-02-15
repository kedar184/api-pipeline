import pytest
from typing import Any, Dict, List, Optional
import aiohttp
from datetime import datetime, UTC

from api_pipeline.core.base import (
    BaseExtractor, BaseOutput, ExtractorConfig,
    OutputConfig, PaginationType, PaginationConfig
)


class TestExtractor(BaseExtractor):
    """Test implementation of BaseExtractor."""
    
    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": item["id"],
            "value": item["value"],
            "processed_at": datetime.now(UTC).isoformat()
        }

    def _get_item_params(self, item: Any) -> Dict[str, Any]:
        return {"id": item} if isinstance(item, (str, int)) else item


class TestOutput(BaseOutput):
    """Test implementation of BaseOutput."""
    
    def __init__(self, config: OutputConfig):
        super().__init__(config)
        self.written_data: List[Dict[str, Any]] = []
        self.is_closed = False

    async def write(self, data: List[Dict[str, Any]]) -> None:
        self.written_data.extend(data)

    async def close(self) -> None:
        self.is_closed = True


@pytest.fixture
def test_extractor_config() -> ExtractorConfig:
    """Provide test extractor configuration."""
    return ExtractorConfig(
        base_url="https://api.test.com",
        endpoints={
            "items": "/items",
            "detail": "/items/{id}"
        },
        auth_type="bearer",
        auth_credentials={"token": "test-token"},
        rate_limit=10,
        retry_count=3,
        batch_size=2,
        max_concurrent_requests=2,
        session_timeout=5,
        pagination=PaginationConfig(
            enabled=True,
            strategy=PaginationType.PAGE_NUMBER,
            page_size=10,
            max_pages=2
        )
    )


@pytest.fixture
def test_output_config() -> OutputConfig:
    """Provide test output configuration."""
    return OutputConfig(
        type="test",
        enabled=True,
        config={"test_param": "test_value"}
    )


class TestBaseExtractor:
    """Test suite for BaseExtractor functionality."""

    @pytest.fixture
    def extractor(self, test_extractor_config):
        """Provide test extractor instance."""
        return TestExtractor(test_extractor_config)

    async def test_ensure_session(self, extractor):
        """Test session initialization."""
        await extractor._ensure_session()
        assert isinstance(extractor.session, aiohttp.ClientSession)
        await extractor.session.close()

    async def test_transform_item(self, extractor):
        """Test item transformation."""
        item = {"id": "123", "value": "test"}
        result = await extractor._transform_item(item)
        assert result["id"] == "123"
        assert result["value"] == "test"
        assert "processed_at" in result

    async def test_process_batch(self, extractor, monkeypatch):
        """Test batch processing."""
        async def mock_request(*args, **kwargs):
            return [{"id": "1", "value": "test1"}, {"id": "2", "value": "test2"}]

        monkeypatch.setattr(extractor, "_paginated_request", mock_request)
        
        batch = ["1", "2"]
        result = await extractor._process_batch(batch)
        assert len(result) == 2
        assert all("processed_at" in item for item in result)

    async def test_get_items_to_process(self, extractor):
        """Test item retrieval from parameters."""
        params = {
            "items": ["1", "2", "3"],
            "other_param": "value"
        }
        items = extractor._get_items_to_process(params)
        assert items == ["1", "2", "3"]

    async def test_paginated_request_page_number(self, extractor, monkeypatch):
        """Test page number based pagination."""
        responses = [
            {"items": [{"id": "1"}], "has_more": True},
            {"items": [{"id": "2"}], "has_more": False}
        ]
        current_page = 0

        async def mock_make_request(*args, **kwargs):
            nonlocal current_page
            result = responses[current_page]
            current_page += 1
            return result

        monkeypatch.setattr(extractor, "_make_request", mock_make_request)
        
        result = await extractor._paginated_request("items")
        assert len(result) == 2
        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"

    async def test_paginated_request_cursor(self, extractor, monkeypatch):
        """Test cursor based pagination."""
        extractor.config.pagination.strategy = PaginationType.CURSOR
        responses = [
            {"items": [{"id": "1"}], "next_cursor": "cursor1"},
            {"items": [{"id": "2"}], "next_cursor": None}
        ]
        current_page = 0

        async def mock_make_request(*args, **kwargs):
            nonlocal current_page
            result = responses[current_page]
            current_page += 1
            return result

        monkeypatch.setattr(extractor, "_make_request", mock_make_request)
        
        result = await extractor._paginated_request("items")
        assert len(result) == 2

    async def test_extract_with_batching(self, extractor, monkeypatch):
        """Test complete extraction process with batching."""
        async def mock_process_batch(batch, params=None):
            return [{"id": id, "value": f"test{id}"} for id in batch]

        monkeypatch.setattr(extractor, "_process_batch", mock_process_batch)
        
        result = await extractor.extract({"items": ["1", "2", "3", "4"]})
        assert len(result) == 4

    async def test_error_handling(self, extractor, monkeypatch):
        """Test error handling during extraction."""
        async def mock_process_batch(batch, params=None):
            if "2" in batch:
                raise Exception("Test error")
            return [{"id": id, "value": f"test{id}"} for id in batch]

        monkeypatch.setattr(extractor, "_process_batch", mock_process_batch)
        
        with pytest.raises(Exception):
            await extractor.extract({"items": ["1", "2", "3"]})

    async def test_session_cleanup(self, extractor):
        """Test session cleanup after extraction."""
        await extractor._ensure_session()
        assert extractor.session is not None
        
        await extractor.extract({"items": []})
        assert extractor.session is None


class TestBaseOutput:
    """Test suite for BaseOutput functionality."""

    @pytest.fixture
    def output(self, test_output_config):
        """Provide test output instance."""
        return TestOutput(test_output_config)

    async def test_write_and_close(self, output):
        """Test basic write and close operations."""
        test_data = [{"id": "1", "value": "test"}]
        await output.write(test_data)
        assert output.written_data == test_data
        
        await output.close()
        assert output.is_closed

    async def test_config_access(self, output):
        """Test access to output configuration."""
        assert output.config.type == "test"
        assert output.config.enabled
        assert output.config.config["test_param"] == "test_value"


class TestPaginationConfig:
    """Test suite for PaginationConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = PaginationConfig()
        assert config.enabled
        assert config.strategy == PaginationType.PAGE_NUMBER
        assert config.page_size == 100
        assert config.max_pages is None

    def test_custom_values(self):
        """Test custom configuration values."""
        config = PaginationConfig(
            enabled=False,
            strategy=PaginationType.CURSOR,
            page_size=50,
            max_pages=5,
            cursor_param="next",
            cursor_field="nextToken"
        )
        assert not config.enabled
        assert config.strategy == PaginationType.CURSOR
        assert config.page_size == 50
        assert config.max_pages == 5
        assert config.cursor_param == "next"
        assert config.cursor_field == "nextToken"

    def test_pagination_types(self):
        """Test all pagination type values."""
        assert set(PaginationType) == {
            PaginationType.PAGE_NUMBER,
            PaginationType.CURSOR,
            PaginationType.OFFSET,
            PaginationType.TOKEN,
            PaginationType.LINK
        } 