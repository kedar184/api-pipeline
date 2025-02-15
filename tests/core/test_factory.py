import pytest
from typing import Dict, List, Type
import os
from unittest.mock import mock_open, patch

from api_pipeline.core.base import BaseOutput, OutputConfig
from api_pipeline.core.factory import PipelineFactory, load_pipeline_config
from api_pipeline.core.models import PipelineConfig


class TestOutput(BaseOutput):
    """Test output implementation."""
    async def write(self, data: List[Dict]) -> None:
        pass

    async def close(self) -> None:
        pass


class TestPipelineFactory:
    """Test suite for PipelineFactory."""

    def setup_method(self):
        """Reset factory state before each test."""
        PipelineFactory._output_handlers = {}
        PipelineFactory._secret_manager = None

    def test_register_output(self):
        """Test output handler registration."""
        PipelineFactory.register_output("test", TestOutput)
        assert "test" in PipelineFactory._output_handlers
        assert PipelineFactory._output_handlers["test"] == TestOutput

        # Test case insensitive registration
        PipelineFactory.register_output("TEST_UPPER", TestOutput)
        assert "test_upper" in PipelineFactory._output_handlers

    def test_create_pipeline(self, base_pipeline_config):
        """Test pipeline creation from configuration."""
        # Register test output
        PipelineFactory.register_output("test_output", TestOutput)

        # Create pipeline config with test output
        config = base_pipeline_config.dict()
        config["output"] = [{
            "type": "test_output",
            "enabled": True,
            "config": {"test": "value"}
        }]

        # Create pipeline
        pipeline = PipelineFactory.create_pipeline(config)
        assert pipeline.pipeline_id == config["pipeline_id"]
        assert len(pipeline.outputs) == 1
        assert isinstance(pipeline.outputs[0], TestOutput)

    def test_create_pipeline_invalid_output(self, base_pipeline_config):
        """Test pipeline creation with invalid output type."""
        config = base_pipeline_config.dict()
        config["output"] = [{
            "type": "invalid_output",
            "config": {}
        }]

        with pytest.raises(ValueError, match="Unsupported output type"):
            PipelineFactory.create_pipeline(config)

    def test_create_pipeline_disabled_output(self, base_pipeline_config):
        """Test pipeline creation with disabled output."""
        PipelineFactory.register_output("test_output", TestOutput)

        config = base_pipeline_config.dict()
        config["output"] = [{
            "type": "test_output",
            "enabled": False,
            "config": {}
        }]

        pipeline = PipelineFactory.create_pipeline(config)
        assert len(pipeline.outputs) == 0

    @patch("builtins.open", new_callable=mock_open, read_data="""
pipeline_id: test-pipeline
description: Test Pipeline
enabled: true
extractor_class: test.TestExtractor
api_config:
  base_url: https://api.test.com
  endpoints:
    test: /test
output: []
    """)
    def test_load_pipeline_config(self, mock_file):
        """Test loading pipeline configuration from file."""
        # Set environment
        os.environ["ENVIRONMENT"] = "dev"

        config = load_pipeline_config("test-pipeline")
        assert isinstance(config, dict)
        assert config["pipeline_id"] == "test-pipeline"
        assert config["enabled"]

    @patch("builtins.open")
    def test_load_pipeline_config_not_found(self, mock_file):
        """Test loading non-existent pipeline configuration."""
        mock_file.side_effect = FileNotFoundError()
        
        with pytest.raises(ValueError, match="Pipeline configuration not found"):
            load_pipeline_config("non-existent")

    @patch("builtins.open")
    def test_load_pipeline_config_invalid_yaml(self, mock_file):
        """Test loading invalid YAML configuration."""
        mock_file.return_value.__enter__.return_value.read.return_value = "invalid: yaml: content"
        
        with pytest.raises(Exception):
            load_pipeline_config("invalid-yaml")

    def test_resolve_secrets(self, base_pipeline_config):
        """Test secret resolution in configuration."""
        # Create config with secret references
        config = base_pipeline_config.dict()
        config["api_config"]["auth_credentials"] = {
            "token": "${secret:projects/test/secrets/token}"
        }

        # Mock secret manager
        class MockSecretManager:
            def get_secret(self, path):
                return "resolved-secret-value"

        PipelineFactory._secret_manager = MockSecretManager()

        # Resolve secrets
        resolved_config = PipelineFactory._resolve_secrets(config)
        assert resolved_config["api_config"]["auth_credentials"]["token"] == "resolved-secret-value"

    def test_import_class(self):
        """Test dynamic class import."""
        # Test with valid import path
        try:
            cls = PipelineFactory._import_class("api_pipeline.core.base.BaseOutput")
            assert cls == BaseOutput
        except ImportError:
            pytest.fail("Failed to import valid class")

        # Test with invalid import path
        with pytest.raises(ValueError, match="Failed to import"):
            PipelineFactory._import_class("invalid.module.Class") 