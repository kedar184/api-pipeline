from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime
import aiohttp
from pydantic import BaseModel, ConfigDict

class ExtractorConfig(BaseModel):
    """Simplified configuration for testing."""
    api_config: Dict[str, Any]
    
    model_config = ConfigDict(arbitrary_types_allowed=True)

class WatermarkConfig(BaseModel):
    """Simplified watermark configuration for testing."""
    enabled: bool = False
    timestamp_field: str = "created_at"
    
    model_config = ConfigDict(arbitrary_types_allowed=True)

class BaseExtractor(ABC):
    """Simplified base extractor for testing."""
    
    def __init__(self, config: ExtractorConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
    
    @abstractmethod
    async def extract(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract data from the API."""
        pass 