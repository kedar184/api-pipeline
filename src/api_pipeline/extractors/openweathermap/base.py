from datetime import datetime, UTC
from typing import Dict, Any, Optional
from loguru import logger

from api_pipeline.core.base import ExtractorConfig
from api_pipeline.core.auth import AuthConfig

class OpenWeatherMapBaseConfig(ExtractorConfig):
    """Base configuration for OpenWeatherMap API extractors"""
    class Config:
        arbitrary_types_allowed = True

    # Units configuration
    units: str = "standard"  # standard (Kelvin), metric (Celsius), or imperial (Fahrenheit)
    
    # Monthly call tracking
    monthly_call_limit: int = 950000  # Buffer below 1M limit
    current_monthly_calls: int = 0
    
    # Per-minute rate limit
    per_minute_limit: int = 50  # Buffer below 60 limit
    current_minute_calls: int = 0
    last_minute_reset: datetime = datetime.now(UTC)

def kelvin_to_celsius(kelvin: float, units: str = "standard") -> float:
    """Convert temperature based on units setting
    
    Args:
        kelvin: Temperature value from API
        units: Unit system (standard, metric, imperial)
        
    Returns:
        Temperature in Celsius
    """
    if units == "metric":
        return kelvin  # Already in Celsius from API
    return kelvin - 273.15  # Convert from Kelvin to Celsius

def validate_coordinates(lat: float, lon: float) -> None:
    """Validate coordinate ranges"""
    if not (-90 <= lat <= 90):
        raise ValueError(f"Invalid latitude: {lat}")
    if not (-180 <= lon <= 180):
        raise ValueError(f"Invalid longitude: {lon}")

def get_location_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract location information from API response"""
    return {
        'name': data.get('name'),
        'country': data.get('sys', {}).get('country'),
        'coordinates': {
            'lat': data.get('coord', {}).get('lat'),
            'lon': data.get('coord', {}).get('lon')
        }
    } 