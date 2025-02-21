import pytest
from datetime import datetime, UTC, timedelta
from api_pipeline.extractors.openweathermap import OpenWeatherMapExtractor, OpenWeatherMapConfig
from api_pipeline.core.auth import AuthConfig
from api_pipeline.core.rate_limiting import RateLimitError

@pytest.fixture
def config():
    return OpenWeatherMapConfig(
        base_url="https://api.openweathermap.org",
        endpoints={
            'current_weather': '/data/2.5/weather',
            'forecast': '/data/2.5/forecast',
            'air_pollution': '/data/2.5/air_pollution'
        },
        auth_config=AuthConfig(
            type="api_key",
            api_key="test_key"
        ),
        temperature_threshold=25.0,
        check_air_quality=True
    )

@pytest.mark.asyncio
async def test_rate_limit_per_minute(config):
    """Test per-minute rate limiting"""
    extractor = OpenWeatherMapExtractor(config)
    
    # Set current calls close to limit
    extractor.config.current_minute_calls = extractor.config.per_minute_limit - 1
    
    # This should work (last allowed call)
    await extractor._check_rate_limits()
    
    # This should raise RateLimitError
    with pytest.raises(RateLimitError):
        await extractor._check_rate_limits()

@pytest.mark.asyncio
async def test_rate_limit_reset(config):
    """Test rate limit counter reset"""
    extractor = OpenWeatherMapExtractor(config)
    
    # Set calls to limit
    extractor.config.current_minute_calls = extractor.config.per_minute_limit
    # Set last reset to more than a minute ago
    extractor.config.last_minute_reset = datetime.now(UTC) - timedelta(minutes=1)
    
    # This should work because counter should reset
    await extractor._check_rate_limits()
    assert extractor.config.current_minute_calls == 1

@pytest.mark.asyncio
async def test_monthly_limit(config):
    """Test monthly call limit"""
    extractor = OpenWeatherMapExtractor(config)
    
    # Set monthly calls to limit
    extractor.config.current_monthly_calls = extractor.config.monthly_call_limit
    
    # This should raise RateLimitError
    with pytest.raises(RateLimitError):
        await extractor._check_rate_limits()

def test_endpoint_validation(config):
    """Test that only free tier endpoints are allowed"""
    # Try to add a non-free endpoint
    config.endpoints['premium_endpoint'] = '/premium/endpoint'
    
    # This should raise ValueError
    with pytest.raises(ValueError):
        OpenWeatherMapExtractor(config)

def test_coordinate_validation(config):
    """Test coordinate validation"""
    extractor = OpenWeatherMapExtractor(config)
    
    # Valid coordinates
    extractor._validate({'lat': 45.0, 'lon': -122.0})
    
    # Invalid latitude
    with pytest.raises(ValueError):
        extractor._validate({'lat': 91.0, 'lon': 0.0})
    
    # Invalid longitude
    with pytest.raises(ValueError):
        extractor._validate({'lat': 0.0, 'lon': 181.0})

@pytest.mark.asyncio
async def test_transform_weather_data(config):
    """Test weather data transformation"""
    extractor = OpenWeatherMapExtractor(config)
    
    # Sample API response
    sample_data = {
        'name': 'London',
        'sys': {'country': 'GB'},
        'coord': {'lat': 51.51, 'lon': -0.13},
        'main': {
            'temp': 293.15,
            'feels_like': 292.15,
            'humidity': 70,
            'pressure': 1013
        },
        'weather': [{'description': 'scattered clouds'}],
        'wind': {'speed': 4.1, 'deg': 250}
    }
    
    transformed = await extractor._transform_item(sample_data)
    
    assert transformed['location']['name'] == 'London'
    assert transformed['location']['country'] == 'GB'
    assert transformed['weather']['temperature'] == 293.15
    assert transformed['weather']['description'] == 'scattered clouds'

@pytest.mark.asyncio
async def test_transform_forecast_data(config):
    """Test forecast data transformation"""
    extractor = OpenWeatherMapExtractor(config)
    
    # Sample forecast response
    sample_forecast = {
        'city': {
            'name': 'London',
            'country': 'GB',
            'coord': {'lat': 51.51, 'lon': -0.13}
        },
        'list': [
            {
                'dt_txt': '2024-03-20 12:00:00',
                'main': {
                    'temp': 295.15,
                    'feels_like': 294.15,
                    'humidity': 65,
                    'pressure': 1015
                },
                'weather': [{'description': 'sunny'}],
                'wind': {'speed': 3.5, 'deg': 180}
            }
        ]
    }
    
    transformed = await extractor._transform_forecast(sample_forecast)
    
    assert transformed['location']['name'] == 'London'
    assert len(transformed['forecast']) == 1
    assert transformed['forecast'][0]['temperature'] == 295.15
    assert transformed['forecast'][0]['description'] == 'sunny'

@pytest.mark.asyncio
async def test_transform_air_pollution_data(config):
    """Test air pollution data transformation"""
    extractor = OpenWeatherMapExtractor(config)
    
    # Sample air pollution response
    sample_pollution = {
        'coord': {'lat': 51.51, 'lon': -0.13},
        'list': [
            {
                'dt': int(datetime.now(UTC).timestamp()),
                'main': {'aqi': 2},
                'components': {
                    'co': 250.34,
                    'no2': 15.68,
                    'pm2_5': 8.69
                }
            }
        ]
    }
    
    transformed = await extractor._transform_air_pollution(sample_pollution)
    
    assert transformed['location']['coordinates']['lat'] == 51.51
    assert len(transformed['air_quality']) == 1
    assert transformed['air_quality'][0]['aqi'] == 2
    assert transformed['air_quality'][0]['components']['co'] == 250.34

@pytest.mark.asyncio
async def test_extract_chain_high_temp(config):
    """Test chain extraction with high temperature (should trigger forecast)"""
    extractor = OpenWeatherMapExtractor(config)
    
    # Mock high temperature response
    async def mock_make_request(endpoint, params=None, endpoint_override=None):
        if endpoint == 'current_weather':
            return {
                'name': 'Dubai',
                'sys': {'country': 'AE'},
                'coord': {'lat': 25.20, 'lon': 55.27},
                'main': {'temp': 308.15},  # 35°C
                'weather': [{'description': 'clear sky'}]
            }
        elif endpoint == 'forecast':
            return {
                'city': {'name': 'Dubai', 'country': 'AE'},
                'list': [{
                    'dt_txt': '2024-03-20 12:00:00',
                    'main': {'temp': 309.15},
                    'weather': [{'description': 'sunny'}]
                }]
            }
        elif endpoint == 'air_pollution':
            return {
                'coord': {'lat': 25.20, 'lon': 55.27},
                'list': [{
                    'dt': int(datetime.now(UTC).timestamp()),
                    'main': {'aqi': 3},
                    'components': {'co': 300.0}
                }]
            }
    
    # Replace real API call with mock
    extractor._make_request = mock_make_request
    
    result = await extractor.extract_chain({'lat': 25.20, 'lon': 55.27})
    
    assert result['current_weather'] is not None
    assert result['forecast'] is not None  # Should have forecast due to high temp
    assert result['air_quality'] is not None

@pytest.mark.asyncio
async def test_extract_chain_low_temp(config):
    """Test chain extraction with low temperature (should skip forecast)"""
    extractor = OpenWeatherMapExtractor(config)
    
    # Mock low temperature response
    async def mock_make_request(endpoint, params=None, endpoint_override=None):
        if endpoint == 'current_weather':
            return {
                'name': 'Reykjavik',
                'sys': {'country': 'IS'},
                'coord': {'lat': 64.14, 'lon': -21.94},
                'main': {'temp': 273.15},  # 0°C
                'weather': [{'description': 'snow'}]
            }
        elif endpoint == 'air_pollution':
            return {
                'coord': {'lat': 64.14, 'lon': -21.94},
                'list': [{
                    'dt': int(datetime.now(UTC).timestamp()),
                    'main': {'aqi': 1},
                    'components': {'co': 150.0}
                }]
            }
    
    # Replace real API call with mock
    extractor._make_request = mock_make_request
    
    result = await extractor.extract_chain({'lat': 64.14, 'lon': -21.94})
    
    assert result['current_weather'] is not None
    assert result['forecast'] is None  # Should not have forecast due to low temp
    assert result['air_quality'] is not None 