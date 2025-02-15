import pytest
from pathlib import Path
from typing import Dict, List, Optional

from api_pipeline.core.base import OutputConfig, ExtractorConfig
from api_pipeline.core.models import PipelineConfig


@pytest.fixture
def test_data_dir(tmp_path) -> Path:
    """Provide a temporary directory for test data."""
    return tmp_path / "data"


@pytest.fixture
def test_config_dir(tmp_path) -> Path:
    """Provide a temporary directory for test configurations."""
    return tmp_path / "config"


@pytest.fixture
def base_extractor_config() -> ExtractorConfig:
    """Provide a base extractor configuration."""
    return ExtractorConfig(
        base_url="https://api.example.com",
        endpoints={
            "test": "/test",
            "items": "/items"
        },
        auth_type="bearer",
        auth_credentials={
            "token": "test-token"
        },
        rate_limit=10,
        retry_count=3
    )


@pytest.fixture
def base_output_config() -> OutputConfig:
    """Provide a base output configuration."""
    return OutputConfig(
        type="test_output",
        enabled=True,
        config={
            "test_param": "test_value"
        }
    )


@pytest.fixture
def base_pipeline_config(base_extractor_config: ExtractorConfig) -> PipelineConfig:
    """Provide a base pipeline configuration."""
    return PipelineConfig(
        pipeline_id="test-pipeline",
        description="Test pipeline",
        enabled=True,
        extractor_class="test.TestExtractor",
        api_config=base_extractor_config.dict(),
        parameters=[],
        output=[]
    )


@pytest.fixture
def sample_weather_data() -> List[Dict]:
    """Provide sample weather data for testing."""
    return [
        {
            "timestamp": "2024-02-12T12:00:00Z",
            "location_id": "5128581",
            "temperature": 15.6,
            "humidity": 65,
            "wind_speed": 4.2,
            "conditions": "partly cloudy"
        },
        {
            "timestamp": "2024-02-12T12:00:00Z",
            "location_id": "2643743",
            "temperature": 12.3,
            "humidity": 78,
            "wind_speed": 5.1,
            "conditions": "light rain"
        }
    ]


@pytest.fixture
def sample_github_data() -> List[Dict]:
    """Provide sample GitHub data for testing."""
    return [
        {
            "repo_id": 123456,
            "repo_name": "test/repo1",
            "repo_stars": 1000,
            "repo_forks": 500,
            "issue_id": 987654,
            "issue_number": 123,
            "issue_title": "Test issue",
            "issue_state": "open",
            "issue_created_at": "2024-02-12T10:30:00Z",
            "issue_updated_at": "2024-02-12T11:45:00Z",
            "issue_closed_at": None
        }
    ]


@pytest.fixture
def mock_response():
    """Provide a mock aiohttp response."""
    class MockResponse:
        def __init__(self, status: int = 200, data: Optional[Dict] = None):
            self.status = status
            self._data = data or {}

        async def json(self):
            return self._data

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

        def raise_for_status(self):
            if self.status >= 400:
                raise Exception(f"HTTP {self.status}")

    return MockResponse 