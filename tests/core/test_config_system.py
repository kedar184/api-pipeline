import pytest
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

from api_pipeline.core.config import (
    ChainConfig,
    AuthConfig,
    BaseConfig,
    EndpointConfig,
    ExtractorConfig,
    ChainedExtractorConfig,
    ChainStep,
    ConditionConfig,
    OutputMapping
)

# Test-specific models to verify generic configuration support
class TestExtractorCustomConfig(BaseModel):
    """Example extractor-specific configuration"""
    threshold: float
    max_items: int
    filters: List[str]

class TestChainCustomConfig(BaseModel):
    """Example chain-level custom configuration"""
    environment: str
    debug_mode: bool
    retry_count: int

@pytest.fixture
def sample_yaml_content(tmp_path):
    """Create a sample YAML file with comprehensive configuration"""
    content = """
name: test_chain
description: Test configuration system
chain_id: test_001
state_dir: /tmp/test
max_parallel_chains: 2

custom_config:
  environment: dev
  debug_mode: true
  retry_count: 3

base_config:
  base_url: https://api.test.com
  rate_limits:
    per_minute: 30
    monthly: 1000
  timeout: 30

extractors:
  first_extractor:
    type: SINGLE
    extractor_class: test.FirstExtractor
    config:
      endpoints:
        data: /api/v1/data
      parameters:
        format: json
    required: true
    input_mapping:
      input_field: source_field
    output_mapping:
      output_field: target_field
    custom_config:
      threshold: 10.5
      max_items: 100
      filters: ["a", "b", "c"]

  second_extractor:
    type: BATCH
    extractor_class: test.SecondExtractor
    config:
      endpoints:
        data: /api/v2/data
    required: false
    input_mapping:
      field_a: source_a
    output_mapping:
      field_b: target_b

chain:
  - extractor: first_extractor
    output_mapping:
      - source: data.value
        target: value
        transform: to_string

  - extractor: second_extractor
    condition:
      field: value
      operator: gt
      value: 100
    output_mapping:
      - source: result
        target: final_result
"""
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(content)
    return str(config_file)

def test_load_config_with_custom_types(sample_yaml_content):
    """Test loading configuration with custom types for both chain and extractors"""
    config = ChainConfig[TestChainCustomConfig].from_yaml(
        sample_yaml_content,
        custom_config_class=TestChainCustomConfig
    )
    
    # Verify chain-level custom config
    assert config.custom_config is not None
    assert config.custom_config.environment == "dev"
    assert config.custom_config.debug_mode is True
    assert config.custom_config.retry_count == 3
    
    # Verify basic chain config
    assert config.name == "test_chain"
    assert config.chain_id == "test_001"
    assert config.max_parallel_chains == 2
    
    # Verify extractors
    assert "first_extractor" in config.extractors
    first_extractor = config.extractors["first_extractor"]
    assert first_extractor.required is True
    assert first_extractor.processing_mode == "single"
    
    # Verify extractor custom config
    first_config = config.get_extractor_config("first_extractor")
    assert "custom_config" in first_config
    custom_config = TestExtractorCustomConfig(**first_config["custom_config"])
    assert custom_config.threshold == 10.5
    assert custom_config.max_items == 100
    assert custom_config.filters == ["a", "b", "c"]

def test_chain_validation(sample_yaml_content):
    """Test chain validation logic"""
    config = ChainConfig.from_yaml(sample_yaml_content)
    
    # Test valid chain
    errors = config.validate_chain()
    assert not errors, "Valid chain should have no errors"
    
    # Test with missing required extractor and invalid condition field
    config.extractors["first_extractor"].required = True
    config.chain = [step for step in config.chain if step.extractor != "first_extractor"]
    errors = config.validate_chain()
    assert len(errors) == 2, "Should have two errors: missing extractor and invalid condition field"
    assert any("Required extractors missing" in error for error in errors), "Should have missing extractor error"
    assert any("not found in previous steps" in error for error in errors), "Should have invalid condition field error"

def test_get_extractor_config(sample_yaml_content):
    """Test getting extractor configuration"""
    config = ChainConfig.from_yaml(sample_yaml_content)
    
    # Test valid extractor
    extractor_config = config.get_extractor_config("first_extractor")
    assert extractor_config["endpoints"]["data"] == "/api/v1/data"
    assert extractor_config["parameters"]["format"] == "json"
    
    # Test with custom config
    assert "custom_config" in extractor_config
    assert extractor_config["custom_config"]["threshold"] == 10.5
    
    # Test invalid extractor
    with pytest.raises(ValueError, match="not found in configuration"):
        config.get_extractor_config("nonexistent_extractor")

def test_get_chain_step(sample_yaml_content):
    """Test getting chain step configuration"""
    config = ChainConfig.from_yaml(sample_yaml_content)
    
    # Test valid step
    step = config.get_chain_step("first_extractor")
    assert step is not None
    assert step.extractor == "first_extractor"
    assert len(step.output_mapping) == 1
    assert step.output_mapping[0]["source"] == "data.value"
    
    # Test step with condition
    step = config.get_chain_step("second_extractor")
    assert step is not None
    assert step.condition is not None
    assert step.condition["field"] == "value"
    assert step.condition["operator"] == "gt"
    assert step.condition["value"] == 100
    
    # Test invalid step
    assert config.get_chain_step("nonexistent") is None 