import pytest
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp
from tests.extractors.test_base import BaseExtractor, ExtractorConfig, WatermarkConfig
from tests.extractors.test_exceptions import ValidationError

class TestApiExtractor(BaseExtractor):
    """Test API extractor implementation following the guide patterns."""
    
    def __init__(self, config: ExtractorConfig):
        super().__init__(config)
        self.base_url = config.api_config.get("base_url")
        self.endpoints = config.api_config.get("endpoints", {})
        
    async def _init_session(self) -> aiohttp.ClientSession:
        """Initialize API session with auth headers."""
        headers = {
            "Authorization": f"Bearer {self.config.api_config['auth_credentials']['token']}",
            "Content-Type": "application/json"
        }
        return aiohttp.ClientSession(headers=headers)
    
    def _validate_parameters(self, parameters: Dict[str, Any]) -> None:
        """Validate required parameters for extraction."""
        if not parameters.get("account_id"):
            raise ValidationError("account_id is required")
            
        if not isinstance(parameters["account_id"], str):
            raise ValidationError("account_id must be a string")
    
    def _transform_user(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform user endpoint response."""
        return {
            "id": item["id"],
            "email": item["email"],
            "name": item.get("full_name", ""),
            "created_at": item["created_at"],
            "updated_at": item.get("updated_at"),
            "status": item.get("status", "active")
        }
    
    def _transform_order(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform order endpoint response."""
        return {
            "id": item["id"],
            "user_id": item["user_id"],
            "amount": float(item["amount"]),
            "currency": item["currency"],
            "status": item["status"],
            "created_at": item["created_at"]
        }
    
    def _transform_item(self, item: Dict[str, Any], endpoint: str) -> Dict[str, Any]:
        """Transform a single API response item based on endpoint type."""
        transforms = {
            "users": self._transform_user,
            "orders": self._transform_order
        }
        
        if endpoint not in transforms:
            raise ValueError(f"Unsupported endpoint: {endpoint}")
            
        return transforms[endpoint](item)
    
    def _filter_by_watermark(self, items: List[Dict[str, Any]], watermark_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Filter items based on watermark configuration."""
        if not watermark_config or not watermark_config.get("enabled"):
            return items
            
        field = watermark_config.get("timestamp_field", "created_at")
        window_start = datetime.now() - timedelta(days=7)  # Example 7-day lookback
        
        return [
            item for item in items 
            if datetime.fromisoformat(item[field]) >= window_start
        ]
    
    async def extract(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract data from test API endpoints."""
        self._validate_parameters(parameters)
        
        results = []
        async with self._init_session() as session:
            for endpoint, path in self.endpoints.items():
                url = f"{self.base_url}{path.format(**parameters)}"
                
                async with session.get(url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Transform each item
                    transformed = [
                        self._transform_item(item, endpoint)
                        for item in data["items"]
                    ]
                    
                    # Apply watermark filtering
                    filtered = self._filter_by_watermark(
                        transformed, 
                        self.config.api_config.get("watermark")
                    )
                    
                    results.extend(filtered)
        
        return results

# Test cases
def test_parameter_validation():
    config = ExtractorConfig(api_config={
        "base_url": "https://api.example.com",
        "auth_credentials": {"token": "test"}
    })
    extractor = TestApiExtractor(config)
    
    with pytest.raises(ValidationError):
        extractor._validate_parameters({})
        
    with pytest.raises(ValidationError):
        extractor._validate_parameters({"account_id": 123})
        
    # Should not raise
    extractor._validate_parameters({"account_id": "test123"})

def test_transform_user():
    config = ExtractorConfig(api_config={})
    extractor = TestApiExtractor(config)
    
    input_item = {
        "id": "u123",
        "email": "test@example.com",
        "full_name": "Test User",
        "created_at": "2024-01-01T00:00:00",
        "status": "active"
    }
    
    result = extractor._transform_user(input_item)
    assert result["id"] == "u123"
    assert result["email"] == "test@example.com"
    assert result["name"] == "Test User"
    assert result["created_at"] == "2024-01-01T00:00:00"
    assert result["status"] == "active"

def test_transform_order():
    config = ExtractorConfig(api_config={})
    extractor = TestApiExtractor(config)
    
    input_item = {
        "id": "o123",
        "user_id": "u456",
        "amount": "99.99",
        "currency": "USD",
        "status": "completed",
        "created_at": "2024-01-01T00:00:00"
    }
    
    result = extractor._transform_order(input_item)
    assert result["id"] == "o123"
    assert result["user_id"] == "u456"
    assert result["amount"] == 99.99
    assert result["currency"] == "USD"
    assert result["status"] == "completed"
    assert result["created_at"] == "2024-01-01T00:00:00"

def test_transform_item_invalid_endpoint():
    config = ExtractorConfig(api_config={})
    extractor = TestApiExtractor(config)
    
    with pytest.raises(ValueError, match="Unsupported endpoint"):
        extractor._transform_item({"id": "test"}, "invalid_endpoint")

def test_watermark_filter():
    config = ExtractorConfig(api_config={})
    extractor = TestApiExtractor(config)
    
    items = [
        {"id": 1, "created_at": (datetime.now() - timedelta(days=1)).isoformat()},
        {"id": 2, "created_at": (datetime.now() - timedelta(days=10)).isoformat()}
    ]
    
    watermark_config = {
        "enabled": True,
        "timestamp_field": "created_at"
    }
    
    filtered = extractor._filter_by_watermark(items, watermark_config)
    assert len(filtered) == 1
    assert filtered[0]["id"] == 1

def test_watermark_filter_disabled():
    config = ExtractorConfig(api_config={})
    extractor = TestApiExtractor(config)
    
    items = [
        {"id": 1, "created_at": (datetime.now() - timedelta(days=1)).isoformat()},
        {"id": 2, "created_at": (datetime.now() - timedelta(days=10)).isoformat()}
    ]
    
    watermark_config = {"enabled": False}
    filtered = extractor._filter_by_watermark(items, watermark_config)
    
    assert len(filtered) == 2
    assert filtered[0]["id"] == 1
    assert filtered[1]["id"] == 2

@pytest.mark.asyncio
async def test_extract_method():
    config = ExtractorConfig(api_config={
        "base_url": "https://api.example.com",
        "endpoints": {
            "users": "/accounts/{account_id}/users"
        },
        "auth_credentials": {"token": "test"},
        "watermark": {
            "enabled": True,
            "timestamp_field": "created_at"
        }
    })
    
    extractor = TestApiExtractor(config)
    
    # Mock aiohttp.ClientSession to avoid actual HTTP requests
    class MockResponse:
        async def __aenter__(self):
            return self
            
        async def __aexit__(self, *args):
            pass
            
        async def json(self):
            return {
                "items": [
                    {
                        "id": "u123",
                        "email": "test@example.com",
                        "full_name": "Test User",
                        "created_at": datetime.now().isoformat(),
                        "status": "active"
                    }
                ]
            }
            
        def raise_for_status(self):
            pass
    
    class MockSession:
        def __init__(self, *args, **kwargs):
            pass
            
        async def __aenter__(self):
            return self
            
        async def __aexit__(self, *args):
            pass
            
        def get(self, *args, **kwargs):
            return MockResponse()
    
    # Patch the _init_session method
    extractor._init_session = lambda: MockSession()
    
    # Test the extract method
    results = await extractor.extract({"account_id": "test123"})
    
    assert len(results) == 1
    assert results[0]["id"] == "u123"
    assert results[0]["email"] == "test@example.com"
    assert results[0]["name"] == "Test User"
    assert "created_at" in results[0]
    assert results[0]["status"] == "active" 