import os
import pytest
import asyncio
import subprocess
import time
import requests
from typing import Dict, Any
from loguru import logger

from api_pipeline.core.config import ChainConfig, WeatherChainConfig
from api_pipeline.core.chain import ChainExecutor
from api_pipeline.core.auth import AuthConfig
from api_pipeline.extractors.openweathermap import (
    CurrentWeatherExtractor,
    ForecastExtractor,
    AirQualityExtractor
)

@pytest.fixture(scope="session")
def mock_server():
    """Start mock server for testing"""
    # Start the mock server
    server = subprocess.Popen(["python", "tests/mocks/mock_weather_server.py"])
    
    # Wait for server to start
    max_retries = 5
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get("http://localhost:8000/docs")
            if response.status_code == 200:
                break
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
        retries += 1
    
    if retries == max_retries:
        raise RuntimeError("Mock server failed to start")
    
    yield
    
    # Cleanup
    server.terminate()
    server.wait()

@pytest.fixture
def config_file(tmp_path):
    """Create a temporary config file for testing"""
    # Create state directory
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    
    config_content = """
name: test_weather_chain
description: Test weather chain configuration
chain_id: test_weather_chain_001
state_dir: {state_dir}

auth:
  type: api_key
  env_var: TEST_WEATHER_API_KEY
  headers_prefix: ApiKey

base_config:
  base_url: http://localhost:8000
  monthly_call_limit: 1000
  per_minute_limit: 30

extractors:
  current_weather:
    type: SINGLE
    extractor_class: api_pipeline.extractors.openweathermap.current.CurrentWeatherExtractor
    config:
      base_url: http://localhost:8000
      endpoints:
        current_weather: /data/2.5/weather
      auth_config:
        auth_type: api_key
        auth_credentials:
          api_key: mock_api_key
        headers_prefix: ApiKey
    required: true
    input_mapping:
      lat: lat
      lon: lon
    output_mapping:
      location.coordinates.lat: lat
      location.coordinates.lon: lon
      weather.temperature_celsius: current_temp_celsius
      timestamp: current_weather_timestamp
      location: current_weather_location
      weather: current_weather_data

  forecast:
    type: SINGLE
    extractor_class: api_pipeline.extractors.openweathermap.forecast.ForecastExtractor
    config:
      base_url: http://localhost:8000
      endpoints:
        forecast: /data/2.5/forecast
      temperature_threshold: 25.0
      auth_config:
        auth_type: api_key
        auth_credentials:
          api_key: mock_api_key
        headers_prefix: ApiKey
    required: false
    input_mapping:
      lat: lat
      lon: lon
      current_temp_celsius: current_temp_celsius
    output_mapping:
      location.coordinates.lat: lat
      location.coordinates.lon: lon
      timestamp: forecast_timestamp
      location: forecast_location
      forecast: forecast_data

  air_quality:
    type: SINGLE
    extractor_class: api_pipeline.extractors.openweathermap.air_quality.AirQualityExtractor
    config:
      base_url: http://localhost:8000
      endpoints:
        air_pollution: /data/2.5/air_pollution
      aqi_threshold: 3
      auth_config:
        auth_type: api_key
        auth_credentials:
          api_key: mock_api_key
        headers_prefix: ApiKey
    required: true
    input_mapping:
      lat: lat
      lon: lon
    output_mapping:
      timestamp: air_quality_timestamp
      location: air_quality_location
      air_quality: air_quality_data

chain:
  - extractor: current_weather
    condition: null
    output_mapping:
      - source: location.coordinates.lat
        target: lat
      - source: location.coordinates.lon
        target: lon
      - source: weather.temperature_celsius
        target: current_temp_celsius
      - source: timestamp
        target: current_weather_timestamp
      - source: location
        target: current_weather_location
      - source: weather
        target: current_weather_data

  - extractor: forecast
    condition:
      field: current_temp_celsius
      operator: gt
      value: 25.0
    output_mapping:
      - source: location.coordinates.lat
        target: lat
      - source: location.coordinates.lon
        target: lon
      - source: timestamp
        target: forecast_timestamp
      - source: location
        target: forecast_location
      - source: forecast
        target: forecast_data

  - extractor: air_quality
    condition: null
    output_mapping:
      - source: timestamp
        target: air_quality_timestamp
      - source: location
        target: air_quality_location
      - source: air_quality
        target: air_quality_data
""".format(state_dir=state_dir)
    
    config_path = tmp_path / "test_config.yaml"
    config_path.write_text(config_content)
    return str(config_path)

@pytest.mark.asyncio
async def test_framework_flow(config_file, mock_server, monkeypatch):
    """Test the complete framework flow"""
    # Mock API key
    monkeypatch.setenv('TEST_WEATHER_API_KEY', 'mock_api_key')
    
    # 1. Load configuration
    logger.info("Loading chain configuration")
    chain_config = ChainConfig.from_yaml(config_file)
    
    # Validate configuration
    errors = chain_config.validate_chain()
    assert not errors, f"Chain validation failed: {errors}"
    
    # 2. Initialize executor
    logger.info("Initializing chain executor")
    executor = ChainExecutor()
    
    # 3. Define test parameters
    test_cities = [
        {
            'name': 'Dubai',
            'lat': 25.2048,
            'lon': 55.2708,
            'expected_forecast': True
        },
        {
            'name': 'Reykjavik',
            'lat': 64.1466,
            'lon': -21.9426,
            'expected_forecast': False
        }
    ]
    
    for city in test_cities:
        logger.info(f"\nTesting chain for {city['name']}")
        
        # 4. Execute chain
        try:
            results = await executor.execute_single_chain(
                chain_config,
                {
                    'lat': city['lat'],
                    'lon': city['lon'],
                    'name': city['name']
                }
            )
            
            # 5. Validate results
            assert '_errors' not in results, f"Chain completed with errors: {results.get('_errors')}"
            
            # Validate current weather
            assert 'current_weather_data' in results, "Missing current weather data"
            current = results['current_weather_data']
            assert 'temperature_celsius' in current, "Missing temperature"
            
            # Validate forecast presence
            has_forecast = 'forecast_data' in results
            assert has_forecast == city['expected_forecast'], \
                f"Forecast presence mismatch for {city['name']}"
            
            # Validate air quality
            assert 'air_quality_data' in results, "Missing air quality data"
            assert len(results['air_quality_data']) > 0, "Invalid air quality format"
            
            # Log results
            logger.info(f"Results for {city['name']}:")
            logger.info(f"Temperature: {current['temperature_celsius']:.1f}°C")
            if has_forecast:
                logger.info("Forecast: Available")
                logger.info("Sample forecast periods:")
                for period in results['forecast_data'][:3]:
                    logger.info(f"- {period['datetime']}: {period['temperature_celsius']:.1f}°C, {period['description']}")
            else:
                logger.info("Forecast: Not available")
            logger.info(f"Air Quality Index: {results['air_quality_data'][0]['aqi']}")
            
        except Exception as e:
            pytest.fail(f"Chain execution failed for {city['name']}: {str(e)}")
        
        # Add delay between cities
        await asyncio.sleep(1)
    
    # 6. Validate metrics
    metrics = executor.get_metrics()
    assert metrics['chains_executed'] == len(test_cities), "Incorrect number of chains executed"
    assert metrics['chains_failed'] == 0, "Some chains failed"
    assert metrics['total_execution_time'] > 0, "Invalid execution time" 