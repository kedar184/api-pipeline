import os
import pytest
import asyncio
from datetime import datetime, UTC
from loguru import logger

from api_pipeline.core.auth import AuthConfig
from api_pipeline.core.chain import ExtractorChain
from api_pipeline.extractors.openweathermap.current import CurrentWeatherConfig, CurrentWeatherExtractor
from api_pipeline.extractors.openweathermap.forecast import ForecastConfig, ForecastExtractor
from api_pipeline.extractors.openweathermap.air_quality import AirQualityConfig, AirQualityExtractor

def get_output_transform(source_field: str, target_field: str = None):
    """Create a simple output transform function"""
    def transform(items: list):
        if not items:
            return {}
        if target_field:
            return {target_field: items[0].get(source_field)}
        return items[0]
    return transform

@pytest.fixture
def api_key():
    """Get API key from environment"""
    api_key = os.getenv('LOCAL_WEATHER_API_KEY')
    if not api_key:
        pytest.skip("LOCAL_WEATHER_API_KEY environment variable not set")
    return api_key

@pytest.fixture
def auth_config(api_key):
    """Create auth configuration"""
    return AuthConfig(
        auth_type="api_key",
        auth_credentials={"api_key": api_key},
        headers_prefix="ApiKey"
    )

@pytest.fixture
def chain(auth_config):
    """Create the extractor chain"""
    # Common base configuration
    base_config = {
        "base_url": "https://api.openweathermap.org",
        "monthly_call_limit": 1000,
        "per_minute_limit": 30
    }
    
    # Create extractors
    current_weather = CurrentWeatherExtractor(
        CurrentWeatherConfig(
            auth_config=auth_config,
            **base_config
        )
    )
    
    forecast = ForecastExtractor(
        ForecastConfig(
            auth_config=auth_config,
            temperature_threshold=25.0,
            **base_config
        )
    )
    
    air_quality = AirQualityExtractor(
        AirQualityConfig(
            auth_config=auth_config,
            aqi_threshold=3,
            **base_config
        )
    )
    
    # Create and configure chain
    chain = ExtractorChain()
    
    # Add current weather extractor
    chain.add_extractor(
        current_weather,
        output_transform=lambda items: {
            'lat': items[0]['location']['coordinates']['lat'],
            'lon': items[0]['location']['coordinates']['lon'],
            'current_temp_celsius': items[0]['weather']['temperature_celsius'],
            'current_weather': items[0]
        }
    )
    
    # Add forecast extractor with condition
    chain.add_extractor(
        forecast,
        condition=lambda data: data.get('current_temp_celsius', 0) > 25.0,
        output_transform=lambda items: {
            'lat': items[0]['location']['coordinates']['lat'],
            'lon': items[0]['location']['coordinates']['lon'],
            'forecast': items[0]
        } if items else {}
    )
    
    # Add air quality extractor
    chain.add_extractor(
        air_quality,
        output_transform=lambda items: {'air_quality': items[0]} if items else {}
    )
    
    return chain

@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_weather_chain(chain):
    """Test the weather chain with real API calls"""
    # Test cities with different climates
    cities = [
        {
            'name': 'Dubai',
            'lat': 25.2048,
            'lon': 55.2708,
            'expected_temp_above_25': True
        },
        {
            'name': 'Reykjavik',
            'lat': 64.1466,
            'lon': -21.9426,
            'expected_temp_above_25': False
        },
        {
            'name': 'Beijing',
            'lat': 39.9042,
            'lon': 116.4074,
            'expected_temp_above_25': True
        }
    ]
    
    for city in cities:
        logger.info(f"\nTesting weather chain for {city['name']}")
        
        # Execute chain
        result = await chain.execute({
            'lat': city['lat'],
            'lon': city['lon']
        })
        
        # Log and validate results
        logger.info(f"\nResults for {city['name']}:")
        
        # Validate current weather
        assert 'current_weather' in result, f"Missing current weather for {city['name']}"
        current = result['current_weather']
        temp_celsius = current['weather']['temperature_celsius']
        logger.info(f"Temperature: {temp_celsius:.1f}°C")
        logger.info(f"Description: {current['weather']['description']}")
        
        # Validate forecast presence based on temperature
        has_forecast = 'forecast' in result
        logger.info(f"Forecast available: {has_forecast}")
        if temp_celsius > 25.0:
            assert has_forecast, f"Missing forecast for {city['name']} despite high temperature ({temp_celsius}°C)"
            # Validate forecast data
            forecast = result['forecast']
            assert len(forecast['forecast']) > 0, "Empty forecast data"
            logger.info("Sample forecast periods:")
            for period in forecast['forecast'][:3]:
                logger.info(f"- {period['datetime']}: {period['temperature_celsius']:.1f}°C, {period['description']}")
        else:
            assert not has_forecast, f"Unexpected forecast for {city['name']} with low temperature ({temp_celsius}°C)"
        
        # Validate air quality
        assert 'air_quality' in result, f"Missing air quality data for {city['name']}"
        air_quality = result['air_quality']
        assert len(air_quality['air_quality']) > 0, "Empty air quality data"
        latest_aqi = air_quality['air_quality'][0]
        logger.info(f"Air Quality Index: {latest_aqi['aqi']}")
        if latest_aqi.get('alert'):
            logger.warning(f"⚠️ Air quality alert for {city['name']}!")
        
        # Add delay between cities to respect rate limits
        await asyncio.sleep(2)

@pytest.mark.integration
@pytest.mark.asyncio
async def test_error_handling(chain):
    """Test error handling with invalid coordinates"""
    invalid_coords = [
        {'lat': 91, 'lon': 0},  # Invalid latitude
        {'lat': 0, 'lon': 181},  # Invalid longitude
        {'lat': -100, 'lon': -190}  # Both invalid
    ]
    
    for coords in invalid_coords:
        logger.info(f"\nTesting invalid coordinates: {coords}")
        with pytest.raises(Exception) as exc:
            await chain.execute(coords)
        logger.info(f"Received expected error: {str(exc.value)}")
        
        # Add delay between attempts
        await asyncio.sleep(1) 