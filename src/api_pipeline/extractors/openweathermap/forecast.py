"""Forecast extractor for OpenWeatherMap API."""
from datetime import datetime, UTC
from typing import Dict, Any, Optional, List
from loguru import logger

from api_pipeline.core.base import BaseExtractor
from api_pipeline.core.types import ProcessingPattern
from api_pipeline.extractors.openweathermap.base import (
    OpenWeatherMapBaseConfig,
    kelvin_to_celsius,
    validate_coordinates,
    get_location_dict
)

class ForecastConfig(OpenWeatherMapBaseConfig):
    """Configuration for forecast endpoint"""
    def __init__(self, **data):
        super().__init__(**data)
        self.endpoints = {'forecast': '/data/2.5/forecast'}
        
    # Chain configuration
    temperature_threshold: float = 25.0  # Only get forecast if current temp > threshold

class ForecastExtractor(BaseExtractor):
    """Extractor for forecast data"""
    
    def __init__(self, config: ForecastConfig):
        if not isinstance(config, ForecastConfig):
            raise ValueError("Config must be ForecastConfig instance")
        super().__init__(config)
        self.config: ForecastConfig = config

    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate parameters"""
        if not parameters:
            raise ValueError("Parameters required")
            
        if 'lat' not in parameters or 'lon' not in parameters:
            raise ValueError("Latitude and longitude required")
            
        validate_coordinates(float(parameters['lat']), float(parameters['lon']))
        
        # Check temperature threshold from previous extractor
        if 'current_temp_celsius' not in parameters:
            raise ValueError("Current temperature required from previous extractor")
        
        current_temp = float(parameters['current_temp_celsius'])
        if current_temp <= self.config.temperature_threshold:
            raise ValueError(
                f"Current temperature {current_temp}°C below threshold "
                f"{self.config.temperature_threshold}°C"
            )

    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform forecast data into standardized format"""
        forecast_data = []
        
        for forecast_item in item.get('list', []):
            temp = forecast_item.get('main', {}).get('temp', 0)
            temp_c = kelvin_to_celsius(temp, self.config.units)
            
            forecast_data.append({
                'datetime': forecast_item.get('dt_txt'),
                'temperature_celsius': temp_c,
                'description': forecast_item.get('weather', [{}])[0].get('description'),
                'humidity': forecast_item.get('main', {}).get('humidity'),
                'pressure': forecast_item.get('main', {}).get('pressure'),
                'wind': {
                    'speed': forecast_item.get('wind', {}).get('speed'),
                    'direction': forecast_item.get('wind', {}).get('deg')
                }
            })
            
        return {
            'forecast_data': forecast_data,
            'lat': item.get('city', {}).get('coord', {}).get('lat'),
            'lon': item.get('city', {}).get('coord', {}).get('lon'),
            'forecast_timestamp': datetime.now(UTC).isoformat(),
            'forecast_location': {
                'name': item.get('city', {}).get('name'),
                'country': item.get('city', {}).get('country'),
                'coordinates': {
                    'lat': item.get('city', {}).get('coord', {}).get('lat'),
                    'lon': item.get('city', {}).get('coord', {}).get('lon')
                }
            }
        } 