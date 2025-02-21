"""Current weather extractor for OpenWeatherMap API."""
from datetime import datetime, UTC
from typing import Dict, Any, Optional
from loguru import logger

from api_pipeline.core.base import BaseExtractor
from api_pipeline.core.types import ProcessingPattern
from api_pipeline.extractors.openweathermap.base import (
    OpenWeatherMapBaseConfig,
    validate_coordinates,
    kelvin_to_celsius
)

class CurrentWeatherConfig(OpenWeatherMapBaseConfig):
    """Configuration for current weather endpoint"""
    def __init__(self, **data):
        super().__init__(**data)
        self.endpoints = {'current': '/data/2.5/weather'}
        
    # Chain configuration
    temp_threshold: float = 30.0  # Alert if temp > threshold (Celsius)

class CurrentWeatherExtractor(BaseExtractor):
    """Extractor for current weather data"""
    
    def __init__(self, config: CurrentWeatherConfig):
        if not isinstance(config, CurrentWeatherConfig):
            raise ValueError("Config must be CurrentWeatherConfig instance")
        super().__init__(config)
        self.config: CurrentWeatherConfig = config

    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate parameters"""
        if not parameters:
            raise ValueError("Parameters required")
            
        if 'lat' not in parameters or 'lon' not in parameters:
            raise ValueError("Latitude and longitude required")
            
        validate_coordinates(float(parameters['lat']), float(parameters['lon']))

    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform current weather data into standardized format"""
        # Extract main weather data
        main_data = item.get('main', {})
        temp = main_data.get('temp')
        temp_celsius = kelvin_to_celsius(temp, self.config.units) if temp is not None else None
        
        # Check temperature threshold
        if temp_celsius and temp_celsius > self.config.temp_threshold:
            logger.warning(
                f"High temperature ({temp_celsius:.1f}°C) detected at "
                f"({item.get('coord', {}).get('lat')}, "
                f"{item.get('coord', {}).get('lon')})"
            )
        
        weather_data = {
            'temperature': {
                'celsius': temp_celsius,
                'kelvin': temp if self.config.units == "standard" else (temp + 273.15 if temp is not None else None)
            },
            'humidity': main_data.get('humidity'),
            'pressure': main_data.get('pressure'),
            'description': item.get('weather', [{}])[0].get('description', ''),
            'wind': {
                'speed': item.get('wind', {}).get('speed'),
                'direction': item.get('wind', {}).get('deg')
            }
        }
            
        return {
            'weather_data': weather_data,
            'lat': item.get('coord', {}).get('lat'),
            'lon': item.get('coord', {}).get('lon'),
            'weather_timestamp': datetime.now(UTC).isoformat(),
            'weather_location': {
                'name': item.get('name'),
                'country': item.get('sys', {}).get('country'),
                'coordinates': {
                    'lat': item.get('coord', {}).get('lat'),
                    'lon': item.get('coord', {}).get('lon')
                }
            }
        } 