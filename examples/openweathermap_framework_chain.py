import asyncio
import os
from datetime import datetime, UTC
from typing import Dict, Any, List
from loguru import logger

from api_pipeline.core.auth import AuthConfig
from api_pipeline.core.chain import ExtractorChain
from api_pipeline.extractors.openweathermap.current import CurrentWeatherConfig, CurrentWeatherExtractor
from api_pipeline.extractors.openweathermap.forecast import ForecastConfig, ForecastExtractor
from api_pipeline.extractors.openweathermap.air_quality import AirQualityConfig, AirQualityExtractor

def transform_current_weather(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Transform current weather output for next chain step"""
    if not items:
        return {}
    
    current = items[0]  # Get first item since current weather returns single item
    return {
        'lat': current['location']['coordinates']['lat'],
        'lon': current['location']['coordinates']['lon'],
        'current_temp_celsius': current['weather']['temperature_celsius'],
        'current_weather': current,
        # Preserve any existing data
        **{k: v for k, v in current.items() if k not in ['location', 'weather']}
    }

def transform_forecast(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Transform forecast output for next chain step"""
    if not items:
        return {}
    
    forecast = items[0]  # Get first item
    return {
        'lat': forecast['location']['coordinates']['lat'],
        'lon': forecast['location']['coordinates']['lon'],
        'forecast': forecast,
        # Preserve existing data
        **{k: v for k, v in forecast.items() if k not in ['location', 'forecast']}
    }

def transform_air_quality(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Transform air quality output for final result"""
    if not items:
        return {}
    
    air_quality = items[0]  # Get first item
    return {
        'air_quality': air_quality,
        # Preserve all existing data
        **{k: v for k, v in air_quality.items() if k != 'air_quality'}
    }

def check_temperature_threshold(data: Dict[str, Any]) -> bool:
    """Check if temperature exceeds threshold for forecast"""
    return data.get('current_temp_celsius', 0) > 25.0

async def main():
    # Get API key from environment variable
    api_key = os.getenv('LOCAL_WEATHER_API_KEY')
    if not api_key:
        raise ValueError("LOCAL_WEATHER_API_KEY environment variable not set")

    # Common auth config
    auth_config = AuthConfig(
        type="api_key",
        api_key=api_key
    )

    # Create extractors with their configs
    current_weather = CurrentWeatherExtractor(
        CurrentWeatherConfig(
            base_url="https://api.openweathermap.org",
            auth_config=auth_config,
            monthly_call_limit=1000,  # Conservative limit
            per_minute_limit=30      # Half of actual limit
        )
    )

    forecast = ForecastExtractor(
        ForecastConfig(
            base_url="https://api.openweathermap.org",
            auth_config=auth_config,
            monthly_call_limit=1000,
            per_minute_limit=30,
            temperature_threshold=25.0  # Get forecast if temp > 25°C
        )
    )

    air_quality = AirQualityExtractor(
        AirQualityConfig(
            base_url="https://api.openweathermap.org",
            auth_config=auth_config,
            monthly_call_limit=1000,
            per_minute_limit=30,
            aqi_threshold=3  # Alert if AQI > 3
        )
    )

    # Create the chain
    chain = ExtractorChain()
    
    # Add extractors with proper output transformations
    chain.add_extractor(
        current_weather,
        output_transform=transform_current_weather
    )
    
    chain.add_extractor(
        forecast,
        condition=check_temperature_threshold,
        output_transform=transform_forecast
    )
    
    chain.add_extractor(
        air_quality,
        output_transform=transform_air_quality
    )

    try:
        # Example cities with different climates
        cities = [
            {
                'name': 'Dubai',  # Hot climate - should trigger forecast
                'lat': 25.2048,
                'lon': 55.2708
            },
            {
                'name': 'Reykjavik',  # Cold climate - shouldn't trigger forecast
                'lat': 64.1466,
                'lon': -21.9426
            },
            {
                'name': 'Beijing',  # City with air quality concerns
                'lat': 39.9042,
                'lon': 116.4074
            }
        ]

        logger.info("Starting chained weather data extraction")
        
        for city in cities:
            try:
                logger.info(f"\nProcessing {city['name']}...")
                
                # Execute the chain
                result = await chain.execute({
                    'lat': city['lat'],
                    'lon': city['lon'],
                    'name': city['name']  # Pass city name through chain
                })
                
                # Pretty print the results
                logger.info(f"\nResults for {city['name']}:")
                
                # Current weather
                if 'current_weather' in result:
                    current = result['current_weather']
                    logger.info(f"Current temperature: {current['weather']['temperature_celsius']:.1f}°C")
                    logger.info(f"Description: {current['weather']['description']}")
                
                # Forecast (if available)
                if 'forecast' in result:
                    logger.info("\nForecast available (temperature above threshold):")
                    for day in result['forecast']['forecast'][:3]:  # Show first 3 periods
                        logger.info(f"- {day['datetime']}: {day['temperature_celsius']:.1f}°C, {day['description']}")
                else:
                    logger.info("\nNo forecast (temperature below threshold)")
                
                # Air Quality (if available)
                if 'air_quality' in result:
                    aqi_data = result['air_quality']
                    if aqi_data['air_quality']:
                        latest = aqi_data['air_quality'][0]
                        logger.info(f"\nAir Quality Index: {latest['aqi']}")
                        if latest.get('alert'):
                            logger.warning("⚠️ Air quality alert!")
                        logger.info("Components:")
                        for component, value in latest['components'].items():
                            logger.info(f"- {component}: {value}")
                
                # Add delay between cities
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Error processing {city['name']}: {str(e)}")
                continue

        # Log metrics from each extractor
        logger.info("\nExtraction metrics:")
        for name, extractor in [
            ("Current Weather", current_weather),
            ("Forecast", forecast),
            ("Air Quality", air_quality)
        ]:
            metrics = extractor.get_metrics()
            logger.info(f"\n{name}:")
            logger.info(f"Requests made: {metrics['requests_made']}")
            logger.info(f"Average processing time: {metrics['total_processing_time'] / max(metrics['requests_made'], 1):.2f}s")

    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 