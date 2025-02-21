import asyncio
import os
from datetime import datetime, UTC
from loguru import logger
import json

from api_pipeline.extractors.openweathermap import OpenWeatherMapExtractor, OpenWeatherMapConfig
from api_pipeline.core.auth import AuthConfig

async def main():
    # Get API key from environment variable
    api_key = os.getenv('OPENWEATHERMAP_API_KEY')
    if not api_key:
        raise ValueError("OPENWEATHERMAP_API_KEY environment variable not set")

    # Create configuration with chaining options
    config = OpenWeatherMapConfig(
        base_url="https://api.openweathermap.org",
        endpoints={
            'current_weather': '/data/2.5/weather',
            'forecast': '/data/2.5/forecast',
            'air_pollution': '/data/2.5/air_pollution'
        },
        auth_config=AuthConfig(
            type="api_key",
            api_key=api_key
        ),
        # Rate limiting
        monthly_call_limit=1000,  # Conservative limit
        per_minute_limit=30,     # Half of the actual limit
        # Chain configuration
        temperature_threshold=25.0,  # Get forecast if temperature > 25°C
        check_air_quality=True      # Always get air quality data
    )

    try:
        async with OpenWeatherMapExtractor(config) as extractor:
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
                    result = await extractor.extract_chain({
                        'lat': city['lat'],
                        'lon': city['lon']
                    })
                    
                    # Pretty print the results
                    logger.info(f"\nResults for {city['name']}:")
                    
                    # Current weather
                    current = result['current_weather']
                    temp_k = current['weather']['temperature']
                    temp_c = temp_k - 273.15
                    logger.info(f"Current temperature: {temp_c:.1f}°C")
                    logger.info(f"Description: {current['weather']['description']}")
                    
                    # Forecast (if available)
                    if result['forecast']:
                        logger.info("\nForecast available (temperature above threshold):")
                        for day in result['forecast']['forecast'][:3]:  # Show first 3 periods
                            temp_k = day['temperature']
                            temp_c = temp_k - 273.15
                            logger.info(f"- {day['datetime']}: {temp_c:.1f}°C, {day['description']}")
                    else:
                        logger.info("\nNo forecast (temperature below threshold)")
                    
                    # Air Quality (if available)
                    if result['air_quality']:
                        latest_aqi = result['air_quality']['air_quality'][0]
                        logger.info(f"\nAir Quality Index: {latest_aqi['aqi']}")
                        logger.info("Components:")
                        for component, value in latest_aqi['components'].items():
                            logger.info(f"- {component}: {value}")
                    else:
                        logger.info("\nNo air quality data available")
                    
                    # Add delay between cities
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing {city['name']}: {str(e)}")
                    continue

            # Log metrics
            metrics = extractor.get_metrics()
            logger.info("\nExtraction metrics:")
            logger.info(f"Total requests made: {metrics['requests_made']}")
            logger.info(f"Remaining monthly calls: {config.monthly_call_limit - metrics['requests_made']}")
            logger.info(f"Average processing time: {metrics['total_processing_time'] / max(metrics['requests_made'], 1):.2f}s")

    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 