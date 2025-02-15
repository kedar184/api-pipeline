from datetime import datetime, UTC
from typing import Any, Dict, List, Optional
import aiohttp
from loguru import logger

from api_pipeline.core.base import BaseExtractor
from api_pipeline.core.auth import create_auth_handler


class WeatherExtractor(BaseExtractor):
    """Extracts weather data from OpenWeatherMap API."""
    
    async def _ensure_session(self):
        """Initialize session with API configuration."""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=self.config.session_timeout)
            # Get auth headers from auth handler
            self.auth_handler = create_auth_handler(self.config.auth_config)
            headers = await self.auth_handler.get_auth_headers()
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers
            )

    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single weather data item."""
        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "location_id": item.get("id"),
            "temperature": item["main"]["temp"],
            "humidity": item["main"]["humidity"],
            "wind_speed": item["wind"]["speed"],
            "conditions": item["weather"][0]["main"]
        }

    def _get_item_params(self, item: Any) -> Dict[str, Any]:
        """Convert location ID to API parameters."""
        return {"id": item}

    async def extract(self, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Extract weather data for specified locations."""
        try:
            await self._ensure_session()
            parameters = parameters or {}
            location_ids = parameters.get("location_ids", [])
            
            if not location_ids:
                logger.warning("No location IDs provided")
                return []

            logger.info(f"Fetching weather data for {len(location_ids)} locations")
            all_data = []
            
            for location_id in location_ids:
                try:
                    data = await self._paginated_request(
                        "current",
                        params={"id": location_id}
                    )
                    
                    # Handle case where API returns single item vs list
                    weather_data = data if isinstance(data, list) else [data]
                    
                    all_data.extend([{
                        "timestamp": datetime.now(UTC).isoformat(),
                        "location_id": location_id,
                        "temperature": item["main"]["temp"],
                        "humidity": item["main"]["humidity"],
                        "wind_speed": item["wind"]["speed"],
                        "conditions": item["weather"][0]["main"]
                    } for item in weather_data])
                    
                except Exception as e:
                    logger.error(f"Failed to fetch weather for location {location_id}: {str(e)}")
                    continue
            
            return all_data
        finally:
            if self.session:
                await self.session.close()
                self.session = None 