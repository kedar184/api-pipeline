import asyncio
import os
from datetime import datetime, UTC
from loguru import logger

from api_pipeline.extractors.openweathermap import OpenWeatherMapExtractor, OpenWeatherMapConfig
from api_pipeline.core.auth import AuthConfig

async def main():
    # Get API key from environment variable
    api_key = os.getenv('OPENWEATHERMAP_API_KEY')
    if not api_key:
        raise ValueError("OPENWEATHERMAP_API_KEY environment variable not set")

    # Create configuration
    config = OpenWeatherMapConfig(
        base_url="https://api.openweathermap.org",
        endpoints={
            'current_weather': '/data/2.5/weather',
            'forecast': '/data/2.5/forecast'
        },
        auth_config=AuthConfig(
            type="api_key",
            api_key=api_key
        ),
        # Optional: Set stricter limits for testing
        monthly_call_limit=1000,  # Very conservative limit
        per_minute_limit=30      # Half of the actual limit
    )

    try:
        async with OpenWeatherMapExtractor(config) as extractor:
            # Example: Get weather for multiple cities
            cities = [
                {'lat': 40.7128, 'lon': -74.0060},  # New York
                {'lat': 51.5074, 'lon': -0.1278},   # London
                {'lat': 35.6762, 'lon': 139.6503},  # Tokyo
            ]

            logger.info("Starting weather data extraction")
            
            for city in cities:
                try:
                    # Get current weather
                    weather_data = await extractor.extract(city)
                    
                    # Log the results
                    if weather_data:
                        logger.info(f"Weather data for coordinates {city}:")
                        logger.info(f"Temperature: {weather_data[0]['weather']['temperature']}")
                        logger.info(f"Description: {weather_data[0]['weather']['description']}")
                    
                    # Add delay between requests (good practice)
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error getting weather for {city}: {str(e)}")
                    continue

            # Log metrics
            metrics = extractor.get_metrics()
            logger.info("Extraction metrics:")
            logger.info(f"Total requests made: {metrics['requests_made']}")
            logger.info(f"Remaining monthly calls: {config.monthly_call_limit - metrics['requests_made']}")
            logger.info(f"Average processing time: {metrics['total_processing_time'] / max(metrics['requests_made'], 1):.2f}s")

    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 