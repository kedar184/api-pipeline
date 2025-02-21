import os
import json
import yaml
import pytest
import asyncio
from pathlib import Path
from datetime import datetime, UTC
from loguru import logger

from api_pipeline.core.chain.executor import ChainExecutor
from api_pipeline.core.chain.models import ChainConfig

# Configuration file path
CONFIG_PATH = "config/dev/sources/openweathermap_chain.yaml"

@pytest.fixture
def output_dir(tmp_path):
    """Create temporary directory for output files"""
    output = tmp_path / "output"
    output.mkdir()
    return output

@pytest.fixture
def chain_state_dir(tmp_path):
    """Create temporary directory for chain state"""
    state_dir = tmp_path / "chain_state"
    state_dir.mkdir()
    return state_dir

# Sample test cities with different climate characteristics
sample_data = [
    {
        "name": "Dubai",
        "lat": 25.2048,
        "lon": 55.2708,
        "expected_forecast": True  # Hot climate, should trigger forecast
    },
    {
        "name": "Reykjavik",
        "lat": 64.1466,
        "lon": -21.9426,
        "expected_forecast": False  # Cool climate, should not trigger forecast
    }
]

@pytest.mark.asyncio
async def test_weather_chain_e2e(output_dir, chain_state_dir):
    """End-to-end test of the weather chain using mock API.
    
    Steps:
    1. Load and configure chain with sample data
    2. Mock server should be running (started separately)
    3. Execute chain
    4. Verify output file
    """
    logger.info("Starting end-to-end test of weather chain")

    # 1. Load and configure chain
    with open(CONFIG_PATH, 'r') as f:
        config_dict = yaml.safe_load(f)

    # Update state directory to use temporary path
    config_dict['state_dir'] = str(chain_state_dir)

    # Create chain configuration
    chain_config = ChainConfig(**config_dict)

    # Create output file path
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"weather_data_{timestamp}.json"

    # 2. Mock server should be running on http://0.0.0.0:8000
    # This is started separately using: python tests/mocks/mock_weather_server.py

    # 3. Execute chain for each city
    executor = ChainExecutor()
    all_results = []

    for city in sample_data:
        logger.info(f"\nProcessing weather data for {city['name']}")

        try:
            # Execute chain
            result = await executor.execute_single_chain(
                chain_config,
                {
                    'lat': city['lat'],
                    'lon': city['lon']
                }
            )

            # Verify result structure
            assert 'current_weather' in result, "Current weather data missing"
            current = result['current_weather']
            assert 'weather_data' in current, "Weather data missing"
            weather = current['weather_data']
            
            # Verify weather data fields
            assert 'temperature' in weather, "Temperature data missing"
            assert 'celsius' in weather['temperature'], "Temperature in Celsius missing"
            assert 'humidity' in weather, "Humidity missing"
            assert 'pressure' in weather, "Pressure missing"
            assert 'description' in weather, "Weather description missing"
            assert 'wind' in weather, "Wind data missing"
            assert 'speed' in weather['wind'], "Wind speed missing"
            assert 'direction' in weather['wind'], "Wind direction missing"

            # Verify forecast based on temperature
            if city['expected_forecast']:
                assert 'forecast' in result, f"Forecast missing for {city['name']}"
                forecast = result['forecast']
                assert forecast is not None and len(forecast['forecast_data']) > 0, "Forecast data empty"
            else:
                assert 'forecast' not in result or result['forecast'] is None, f"Unexpected forecast for {city['name']}"

            # Verify air quality data
            assert 'air_quality' in result, "Air quality data missing"
            air_quality = result['air_quality']
            assert 'air_quality_data' in air_quality, "Air quality data missing"
            aqi_data = air_quality['air_quality_data']
            assert isinstance(aqi_data, list) and len(aqi_data) > 0, "Air quality data list is empty"
            first_aqi_data = aqi_data[0]  # Get the first air quality reading
            assert 'aqi' in first_aqi_data, "AQI missing"
            assert 'components' in first_aqi_data, "Air quality components missing"

            all_results.append({
                'city': city['name'],
                'data': result
            })

            logger.info(f"Successfully processed data for {city['name']}")

        except Exception as e:
            logger.error(f"Error processing {city['name']}: {str(e)}")
            raise

    # Write results to output file
    with open(output_file, 'w') as f:
        json.dump(all_results, f, indent=2)

    # Verify output file exists and contains data
    assert output_file.exists(), "Output file not created"
    assert output_file.stat().st_size > 0, "Output file is empty"

    logger.info("End-to-end test completed successfully") 