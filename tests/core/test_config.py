import pytest
from pathlib import Path
import yaml
from typing import Dict, Any

from api_pipeline.core.config import (
    ChainConfig, 
    AuthConfig, 
    BaseConfig, 
    ExtractorConfig, 
    ChainStep, 
    ConditionConfig,
    WeatherChainConfig,
    WeatherConfig,
    TestCity
)

@pytest.fixture
def sample_config() -> Dict[str, Any]:
    """Sample configuration dictionary"""
    return {
        "name": "test_chain",
        "description": "Test chain configuration",
        "auth": {
            "type": "api_key",
            "env_var": "TEST_API_KEY",
            "headers_prefix": "ApiKey"
        },
        "base_config": {
            "base_url": "https://api.test.com",
            "rate_limits": {
                "per_minute": 30,
                "monthly": 1000
            },
            "timeout": 30
        },
        "extractors": {
            "data_extractor": {
                "type": "test",
                "config": {
                    "endpoints": {
                        "data": "/api/v1/data"
                    },
                    "parameters": {
                        "format": "json"
                    }
                },
                "required": True
            }
        },
        "chain": [
            {
                "extractor": "data_extractor",
                "output_mapping": [
                    {
                        "source": "data.value",
                        "target": "value"
                    }
                ]
            }
        ]
    }

@pytest.fixture
def sample_weather_config(sample_config: Dict[str, Any]) -> Dict[str, Any]:
    """Sample weather chain configuration"""
    return {
        **sample_config,
        "custom_config": {
            "units": "metric",
            "temperature_threshold": 25.0,
            "aqi_threshold": 3,
            "test_cities": [
                {
                    "name": "Test City",
                    "description": "Test description",
                    "lat": 0.0,
                    "lon": 0.0,
                    "expected_temp_above_threshold": True,
                    "expected_aqi_alert": False
                }
            ]
        }
    }

@pytest.fixture
def config_file(tmp_path: Path, sample_config: Dict[str, Any]) -> Path:
    """Create a temporary config file"""
    config_file = tmp_path / "test_config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(sample_config, f)
    return config_file

@pytest.fixture
def weather_config_file(tmp_path: Path, sample_weather_config: Dict[str, Any]) -> Path:
    """Create a temporary weather config file"""
    config_file = tmp_path / "test_weather_config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(sample_weather_config, f)
    return config_file

def test_load_basic_config(config_file: Path):
    """Test loading basic chain configuration"""
    config = ChainConfig.from_yaml(str(config_file))
    
    # Verify basic attributes
    assert config.name == "test_chain"
    assert config.description == "Test chain configuration"
    
    # Verify auth config
    assert isinstance(config.auth, AuthConfig)
    assert config.auth.type == "api_key"
    assert config.auth.env_var == "TEST_API_KEY"
    
    # Verify base config
    assert isinstance(config.base_config, BaseConfig)
    assert config.base_config.base_url == "https://api.test.com"
    assert config.base_config.rate_limits == {"per_minute": 30, "monthly": 1000}
    
    # Verify extractors
    assert "data_extractor" in config.extractors
    extractor = config.extractors["data_extractor"]
    assert isinstance(extractor, ExtractorConfig)
    assert extractor.type == "test"
    assert extractor.config.endpoints == {"data": "/api/v1/data"}
    
    # Verify chain steps
    assert len(config.chain) == 1
    step = config.chain[0]
    assert isinstance(step, ChainStep)
    assert step.extractor == "data_extractor"
    assert len(step.output_mapping) == 1
    assert step.output_mapping[0].source == "data.value"
    assert step.output_mapping[0].target == "value"

def test_load_weather_config(weather_config_file: Path):
    """Test loading weather chain configuration"""
    config = WeatherChainConfig.from_yaml(str(weather_config_file))
    
    # Verify custom weather config
    assert config.custom_config is not None
    assert isinstance(config.custom_config, WeatherConfig)
    assert config.custom_config.units == "metric"
    assert config.custom_config.temperature_threshold == 25.0
    assert config.custom_config.aqi_threshold == 3
    
    # Verify test cities
    assert len(config.custom_config.test_cities) == 1
    city = config.custom_config.test_cities[0]
    assert isinstance(city, TestCity)
    assert city.name == "Test City"
    assert city.lat == 0.0
    assert city.lon == 0.0
    assert city.expected_temp_above_threshold is True
    assert city.expected_aqi_alert is False

def test_validate_chain():
    """Test chain validation"""
    config = ChainConfig(
        name="test",
        description="test",
        auth=AuthConfig(type="api_key", env_var="TEST_KEY"),
        base_config=BaseConfig(base_url="https://test.com"),
        extractors={
            "first": ExtractorConfig(
                type="test",
                config={"endpoints": {"test": "/test"}},
                required=True
            ),
            "second": ExtractorConfig(
                type="test",
                config={"endpoints": {"test": "/test"}},
                required=True
            )
        },
        chain=[
            ChainStep(
                extractor="first",
                output_mapping=[{"source": "data", "target": "value"}]
            ),
            ChainStep(
                extractor="second",
                condition=ConditionConfig(
                    field="missing_field",
                    operator="gt",
                    value=10
                ),
                output_mapping=[{"source": "data", "target": "result"}]
            )
        ]
    )
    
    errors = config.validate_chain()
    assert len(errors) == 1
    assert "not found in previous steps" in errors[0]

def test_get_extractor_config():
    """Test getting extractor configuration"""
    config = ChainConfig(
        name="test",
        description="test",
        auth=AuthConfig(type="api_key", env_var="TEST_KEY"),
        base_config=BaseConfig(
            base_url="https://test.com",
            rate_limits={"per_minute": 30}
        ),
        extractors={
            "test_extractor": ExtractorConfig(
                type="test",
                config={
                    "endpoints": {"test": "/test"},
                    "parameters": {"format": "json"}
                }
            )
        },
        chain=[
            ChainStep(
                extractor="test_extractor",
                output_mapping=[{"source": "data", "target": "value"}]
            )
        ]
    )
    
    extractor_config = config.get_extractor_config("test_extractor")
    assert extractor_config["base_url"] == "https://test.com"
    assert extractor_config["endpoints"] == {"test": "/test"}
    assert extractor_config["parameters"] == {"format": "json"}

def test_missing_extractor():
    """Test error when getting config for non-existent extractor"""
    config = ChainConfig(
        name="test",
        description="test",
        auth=AuthConfig(type="api_key", env_var="TEST_KEY"),
        base_config=BaseConfig(base_url="https://test.com"),
        extractors={},
        chain=[]
    )
    
    with pytest.raises(ValueError, match="not found in configuration"):
        config.get_extractor_config("missing_extractor") 