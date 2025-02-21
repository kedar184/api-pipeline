import os
import pytest
import yaml
import asyncio
import subprocess
import time
import operator
from typing import Dict, Any, Generator, List
import requests
from loguru import logger

from api_pipeline.core.auth import AuthConfig
from api_pipeline.core.chain import ExtractorChain
from api_pipeline.extractors.openweathermap.current import CurrentWeatherConfig, CurrentWeatherExtractor
from api_pipeline.extractors.openweathermap.forecast import ForecastConfig, ForecastExtractor
from api_pipeline.extractors.openweathermap.air_quality import AirQualityConfig, AirQualityExtractor

# Constants
MOCK_CONFIG_PATH = "tests/mocks/mock_config.yaml"
MOCK_SERVER_PATH = "tests/mocks/mock_weather_server.py"

@pytest.fixture(scope="session")
def mock_server() -> Generator[None, None, None]:
    """Start mock server for testing"""
    # Start the mock server
    server = subprocess.Popen(["python", MOCK_SERVER_PATH])
    
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
def config() -> Dict[str, Any]:
    """Load mock configuration"""
    with open(MOCK_CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

@pytest.fixture
def auth_config(config) -> AuthConfig:
    """Create auth config"""
    return AuthConfig(
        auth_type=config['auth']['auth_type'],
        auth_credentials=config['auth']['auth_credentials'],
        headers_prefix=config['auth']['headers_prefix']
    )

@pytest.fixture
def extractors(config, auth_config):
    """Create extractors from config"""
    base_config = config['base_config']
    extractors = {}
    
    # Create extractors
    extractors['current_weather'] = CurrentWeatherExtractor(
        CurrentWeatherConfig(
            base_url=base_config['base_url'],
            auth_config=auth_config,
            monthly_call_limit=base_config['monthly_call_limit'],
            per_minute_limit=base_config['per_minute_limit'],
            **config['extractors']['current_weather']
        )
    )

    extractors['forecast'] = ForecastExtractor(
        ForecastConfig(
            base_url=base_config['base_url'],
            auth_config=auth_config,
            monthly_call_limit=base_config['monthly_call_limit'],
            per_minute_limit=base_config['per_minute_limit'],
            **config['extractors']['forecast']
        )
    )

    extractors['air_quality'] = AirQualityExtractor(
        AirQualityConfig(
            base_url=base_config['base_url'],
            auth_config=auth_config,
            monthly_call_limit=base_config['monthly_call_limit'],
            per_minute_limit=base_config['per_minute_limit'],
            **config['extractors']['air_quality']
        )
    )
    
    return extractors

@pytest.fixture
def chain(config, extractors):
    """Create chain from config"""
    chain = ExtractorChain()
    
    # Add extractors according to chain configuration
    for step in config['chain']:
        extractor = extractors[step['extractor']]
        
        chain_args = {
            'output_transform': create_output_transform(step['output_mapping'])
        }
        
        # Add condition if specified
        if 'condition' in step:
            chain_args['condition'] = create_condition(step['condition'])
        
        chain.add_extractor(extractor, **chain_args)
    
    return chain

def get_nested_value(data: Dict[str, Any], path: str) -> Any:
    """Get value from nested dictionary using dot notation"""
    if path == 'self':
        return data
    
    keys = path.split('.')
    value = data
    for key in keys:
        if isinstance(value, dict):
            value = value.get(key)
        else:
            return None
    return value

def create_output_transform(mapping: List[Dict[str, str]]):
    """Create output transform function from mapping configuration"""
    def transform(items: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not items:
            return {}
        
        result = {}
        item = items[0]  # Get first item since we're dealing with single responses
        
        for map_item in mapping:
            value = get_nested_value(item, map_item['source'])
            if value is not None:
                result[map_item['target']] = value
                
        return result
    
    return transform

def create_condition(condition_config: Dict[str, Any]):
    """Create condition function from configuration"""
    operators = {
        'gt': operator.gt,
        'lt': operator.lt,
        'eq': operator.eq,
        'ge': operator.ge,
        'le': operator.le,
    }
    
    op = operators[condition_config['operator']]
    field = condition_config['field']
    value = condition_config['value']
    
    return lambda data: op(data.get(field, 0), value)

@pytest.mark.asyncio
async def test_weather_chain(mock_server, config, chain):
    """Test the complete weather chain for all test cities"""
    for city in config['test_cities']:
        logger.info(f"\nTesting chain for {city['name']} ({city['description']})")
        
        # Execute chain
        result = await chain.execute({
            'lat': city['lat'],
            'lon': city['lon'],
            'name': city['name']
        })
        
        # Validate results match expectations
        expected = city['expected']
        
        # Current weather (required)
        assert ('current_weather' in result) == expected['current_weather'], \
            f"Current weather presence mismatch for {city['name']}"
        if expected['current_weather']:
            current = result['current_weather']
            assert current['weather']['temperature_celsius'] is not None
            assert current['weather']['description'] is not None
        
        # Forecast (conditional)
        assert ('forecast' in result) == expected['forecast'], \
            f"Forecast presence mismatch for {city['name']}"
        if expected['forecast']:
            assert len(result['forecast']['forecast']) > 0
        
        # Air quality (required)
        assert ('air_quality' in result) == expected['air_quality'], \
            f"Air quality presence mismatch for {city['name']}"
        if expected['air_quality']:
            aqi_data = result['air_quality']
            assert len(aqi_data['air_quality']) > 0
            
            # Check for air quality alerts
            aqi_threshold = config['extractors']['air_quality']['config']['aqi_threshold']
            latest_aqi = aqi_data['air_quality'][0]
            if latest_aqi['aqi'] > aqi_threshold:
                assert latest_aqi['alert']
        
        # Log results
        logger.info(f"Results for {city['name']}:")
        if 'current_weather' in result:
            logger.info(f"Temperature: {result['current_weather']['weather']['temperature_celsius']:.1f}°C")
        if 'forecast' in result:
            logger.info("Forecast: Available")
        else:
            logger.info("Forecast: Not available")
        if 'air_quality' in result:
            logger.info(f"Air Quality Index: {result['air_quality']['air_quality'][0]['aqi']}")
        
        # Add delay between cities
        await asyncio.sleep(1) 