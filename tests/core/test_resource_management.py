import pytest
import asyncio
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, UTC

from api_pipeline.core.base import (
    BaseExtractor,
    ExtractorConfig,
    ProcessingPattern
)

class SlowMockExtractor(BaseExtractor):
    """Mock extractor that simulates slow API calls."""
    
    def __init__(self, config: ExtractorConfig):
        super().__init__(config)
        self.call_times: List[float] = []
    
    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return item
    
    async def extract(self, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        # Record the call time
        self.call_times.append(time.monotonic())
        
        # Simulate API call delay
        await asyncio.sleep(0.1)
        
        return [{"id": 1, "value": "test"}]

class ResourceIntensiveExtractor(BaseExtractor):
    """Mock extractor that simulates resource-intensive operations."""
    
    def __init__(self, config: ExtractorConfig):
        super().__init__(config)
        self.concurrent_calls = 0
        self.max_concurrent_calls = 0
    
    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return item
    
    async def extract(self, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        # Track concurrent calls
        self.concurrent_calls += 1
        self.max_concurrent_calls = max(self.max_concurrent_calls, self.concurrent_calls)
        
        try:
            # Simulate work
            await asyncio.sleep(0.2)
            return [{"id": 1, "value": "test"}]
        finally:
            self.concurrent_calls -= 1

@pytest.fixture
def rate_limited_config() -> ExtractorConfig:
    """Config with rate limiting."""
    return ExtractorConfig(
        base_url="https://api.test.com",
        endpoints={"test": "/test"},
        auth_config={"auth_type": "none"},
        processing_pattern=ProcessingPattern.SINGLE,
        rate_limit=5,  # 5 requests per second
        max_concurrent_requests=2
    )

@pytest.fixture
def resource_intensive_config() -> ExtractorConfig:
    """Config for resource-intensive operations."""
    return ExtractorConfig(
        base_url="https://api.test.com",
        endpoints={"test": "/test"},
        auth_config={"auth_type": "none"},
        processing_pattern=ProcessingPattern.SINGLE,
        max_concurrent_requests=3,
        session_timeout=5
    )

class TestRateLimiting:
    """Test suite for rate limiting functionality."""
    
    async def test_rate_limit_enforcement(self, rate_limited_config):
        """Test that rate limits are enforced."""
        extractor = SlowMockExtractor(rate_limited_config)
        
        # Make multiple concurrent requests
        tasks = [
            extractor.extract({"test": i})
            for i in range(10)
        ]
        
        await asyncio.gather(*tasks)
        
        # Calculate request rates
        call_intervals = [
            t2 - t1 
            for t1, t2 in zip(extractor.call_times[:-1], extractor.call_times[1:])
        ]
        
        # Verify minimum interval between requests
        min_expected_interval = 1.0 / rate_limited_config.rate_limit
        assert all(interval >= min_expected_interval for interval in call_intervals)
    
    async def test_concurrent_request_limit(self, rate_limited_config):
        """Test that concurrent request limits are enforced."""
        extractor = ResourceIntensiveExtractor(rate_limited_config)
        
        # Make multiple concurrent requests
        tasks = [
            extractor.extract({"test": i})
            for i in range(5)
        ]
        
        await asyncio.gather(*tasks)
        
        # Verify max concurrent requests
        assert extractor.max_concurrent_calls <= rate_limited_config.max_concurrent_requests

class TestResourceManagement:
    """Test suite for resource management functionality."""
    
    async def test_session_cleanup(self, resource_intensive_config):
        """Test that sessions are properly cleaned up."""
        extractor = ResourceIntensiveExtractor(resource_intensive_config)
        
        async with extractor:
            assert extractor.session is not None
            await extractor.extract()
        
        # Session should be closed after context exit
        assert extractor.session is None or extractor.session.closed
    
    async def test_timeout_handling(self, resource_intensive_config):
        """Test that timeouts are enforced."""
        # Set a very short timeout
        resource_intensive_config.session_timeout = 0.1
        
        extractor = ResourceIntensiveExtractor(resource_intensive_config)
        
        # Request should timeout
        with pytest.raises(asyncio.TimeoutError):
            async with extractor:
                await extractor.extract()
    
    async def test_concurrent_resource_limits(self, resource_intensive_config):
        """Test that resource limits are enforced across concurrent operations."""
        extractor = ResourceIntensiveExtractor(resource_intensive_config)
        
        # Track peak memory usage
        initial_memory = 0  # In a real implementation, we'd track actual memory
        peak_memory = 0
        
        async def monitored_extract(i: int):
            nonlocal peak_memory
            await extractor.extract({"test": i})
            # In a real implementation, we'd measure actual memory usage
            current_memory = initial_memory + (extractor.concurrent_calls * 1000)
            peak_memory = max(peak_memory, current_memory)
        
        # Make concurrent requests
        tasks = [
            monitored_extract(i)
            for i in range(5)
        ]
        
        await asyncio.gather(*tasks)
        
        # Verify resource constraints
        max_expected_memory = initial_memory + (resource_intensive_config.max_concurrent_requests * 1000)
        assert peak_memory <= max_expected_memory
    
    async def test_graceful_shutdown(self, resource_intensive_config):
        """Test that resources are released gracefully on shutdown."""
        extractor = ResourceIntensiveExtractor(resource_intensive_config)
        
        # Start some long-running operations
        tasks = [
            asyncio.create_task(extractor.extract({"test": i}))
            for i in range(3)
        ]
        
        # Wait a bit for tasks to start
        await asyncio.sleep(0.1)
        
        # Cleanup should wait for operations to complete
        await extractor.cleanup()
        
        # Verify all operations completed
        assert extractor.concurrent_calls == 0
        
        # Cancel any remaining tasks
        for task in tasks:
            if not task.done():
                task.cancel()
    
    async def test_resource_isolation(self, resource_intensive_config):
        """Test that resources are properly isolated between extractors."""
        extractor1 = ResourceIntensiveExtractor(resource_intensive_config)
        extractor2 = ResourceIntensiveExtractor(resource_intensive_config)
        
        async with extractor1, extractor2:
            # Run extractions concurrently
            await asyncio.gather(
                extractor1.extract({"test": 1}),
                extractor2.extract({"test": 2})
            )
            
            # Verify each extractor's resources are separate
            assert extractor1.session is not extractor2.session
            assert extractor1.concurrent_calls <= resource_intensive_config.max_concurrent_requests
            assert extractor2.concurrent_calls <= resource_intensive_config.max_concurrent_requests 