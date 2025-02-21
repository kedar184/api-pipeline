import os
import pytest
import yaml
import operator
from datetime import datetime, UTC
from typing import Dict, Any, List, Callable
from loguru import logger

from api_pipeline.core.auth import AuthConfig
from api_pipeline.core.chain import ExtractorChain
from api_pipeline.extractors.openweathermap.current import CurrentWeatherConfig, CurrentWeatherExtractor
from api_pipeline.extractors.openweathermap.forecast import ForecastConfig, ForecastExtractor
from api_pipeline.extractors.openweathermap.air_quality import AirQualityConfig, AirQualityExtractor

# Load config file
CONFIG_PATH = "config/dev/sources/openweathermap_chain.yaml"

def load_config() -> Dict[str, Any]:
    """Load test configuration"""
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

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

def create_output_transform(mapping: List[Dict[str, str]]) -> Callable[[List[Dict[str, Any]]], Dict[str, Any]]:
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

def create_condition(condition_config: Dict[str, Any]) -> Callable[[Dict[str, Any]], bool]:
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

@pytest.fixture
def config():
    """Config fixture"""
    return load_config()

@pytest.fixture
def auth_config(config):
    """Auth config fixture"""
    api_key = os.getenv(config['auth']['env_var'])
    if not api_key:
        pytest.skip(f"Environment variable {config['auth']['env_var']} not set")
    
    return AuthConfig(
        type=config['auth']['type'],
        api_key=api_key
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

@pytest.mark.asyncio
@pytest.mark.integration
async def test_weather_chain(config, chain):
    """Test the complete weather chain for all test cities"""
    for city in config['test_cities']:
        logger.info(f"\nTesting chain for {city['name']} ({city['description']})")
        
        # Execute chain
        result = await chain.execute({
            'lat': city['lat'],
            'lon': city['lon'],
            'name': city['name']
        })
        
        # Validate current weather (required)
        assert 'current_weather' in result, f"Current weather missing for {city['name']}"
        current = result['current_weather']
        assert current['weather']['temperature_celsius'] is not None
        assert current['weather']['description'] is not None
        
        # Validate forecast based on temperature
        temp_celsius = current['weather']['temperature_celsius']
        temp_threshold = config['extractors']['forecast']['config']['temperature_threshold']
        
        if temp_celsius > temp_threshold:
            assert 'forecast' in result, f"Forecast missing for {city['name']} despite high temperature ({temp_celsius}°C)"
            assert len(result['forecast']['forecast']) > 0
        else:
            assert 'forecast' not in result, f"Forecast present for {city['name']} despite low temperature ({temp_celsius}°C)"
        
        # Validate air quality (required)
        assert 'air_quality' in result, f"Air quality missing for {city['name']}"
        aqi_data = result['air_quality']
        assert len(aqi_data['air_quality']) > 0
        
        # Check for air quality alerts
        aqi_threshold = config['extractors']['air_quality']['config']['aqi_threshold']
        latest_aqi = aqi_data['air_quality'][0]
        if latest_aqi['aqi'] > aqi_threshold:
            assert latest_aqi['alert'], f"Missing alert for high AQI ({latest_aqi['aqi']}) in {city['name']}"
        
        # Log results
        logger.info(f"Temperature: {temp_celsius:.1f}°C")
        if 'forecast' in result:
            logger.info("Forecast: Available")
        else:
            logger.info("Forecast: Skipped (temperature below threshold)")
        logger.info(f"Air Quality Index: {latest_aqi['aqi']}")
        
        # Add delay between cities
        await asyncio.sleep(2) 