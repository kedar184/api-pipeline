"""Air quality extractor for OpenWeatherMap API."""
from datetime import datetime, UTC
from typing import Dict, Any, Optional, List
from loguru import logger

from api_pipeline.core.base import BaseExtractor
from api_pipeline.core.types import ProcessingPattern
from api_pipeline.extractors.openweathermap.base import (
    OpenWeatherMapBaseConfig,
    validate_coordinates
)

class AirQualityConfig(OpenWeatherMapBaseConfig):
    """Configuration for air quality endpoint"""
    def __init__(self, **data):
        super().__init__(**data)
        self.endpoints = {'air_pollution': '/data/2.5/air_pollution'}
        
    # Chain configuration
    aqi_threshold: int = 3  # Alert if AQI > threshold

class AirQualityExtractor(BaseExtractor):
    """Extractor for air quality data"""
    
    def __init__(self, config: AirQualityConfig):
        if not isinstance(config, AirQualityConfig):
            raise ValueError("Config must be AirQualityConfig instance")
        super().__init__(config)
        self.config: AirQualityConfig = config

    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate parameters"""
        if not parameters:
            raise ValueError("Parameters required")
            
        if 'lat' not in parameters or 'lon' not in parameters:
            raise ValueError("Latitude and longitude required")
            
        validate_coordinates(float(parameters['lat']), float(parameters['lon']))

    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform air quality data into standardized format"""
        air_quality_data = []
        
        for aqi_item in item.get('list', []):
            aqi = aqi_item.get('main', {}).get('aqi')
            aqi_data = {
                'datetime': datetime.fromtimestamp(aqi_item.get('dt', 0), UTC).isoformat(),
                'aqi': aqi,
                'components': aqi_item.get('components', {}),
                'alert': bool(aqi and aqi > self.config.aqi_threshold)
            }
            air_quality_data.append(aqi_data)
            
            # Log alert if AQI is high
            if aqi_data['alert']:
                logger.warning(
                    f"High Air Quality Index ({aqi}) detected at "
                    f"({item.get('coord', {}).get('lat')}, "
                    f"{item.get('coord', {}).get('lon')})"
                )
            
        return {
            'air_quality_data': air_quality_data,
            'lat': item.get('coord', {}).get('lat'),
            'lon': item.get('coord', {}).get('lon'),
            'air_quality_timestamp': datetime.now(UTC).isoformat(),
            'air_quality_location': {
                'coordinates': {
                    'lat': item.get('coord', {}).get('lat'),
                    'lon': item.get('coord', {}).get('lon')
                }
            }
        } 