import os
from typing import Dict, Type, Any, Union

import yaml
from loguru import logger

from api_pipeline.core.base import BaseOutput, ExtractorConfig, OutputConfig
from api_pipeline.core.models import PipelineConfig
from api_pipeline.core.pipeline import Pipeline
from api_pipeline.core.secrets import SecretManager


class PipelineFactory:
    """Factory for creating pipeline instances."""

    _output_handlers: Dict[str, Type[BaseOutput]] = {}
    _instance = None
    _secret_manager = SecretManager(test_mode=True)

    @classmethod
    def register_output(cls, type_name: str, handler_class: Type[BaseOutput]) -> None:
        """Register an output handler type."""
        cls._output_handlers[type_name.lower()] = handler_class

    @classmethod
    def _resolve_secrets(cls, value: Any) -> Any:
        """Recursively resolve secrets in configuration values."""
        if isinstance(value, str) and value.startswith("${secret:"):
            # Extract secret path from ${secret:path} format
            secret_path = value[9:-1]
            return cls._secret_manager.get_secret(secret_path)
        elif isinstance(value, dict):
            return {k: cls._resolve_secrets(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [cls._resolve_secrets(v) for v in value]
        return value

    @classmethod
    def create_pipeline(cls, config: Dict) -> Pipeline:
        """Create a pipeline instance from configuration."""
        # Resolve secrets in configuration
        config = cls._resolve_secrets(config)
        
        # Validate configuration
        pipeline_config = PipelineConfig(**config)
        
        # Create extractor
        extractor_class = cls._import_class(pipeline_config.extractor_class)
        extractor = extractor_class(ExtractorConfig(**pipeline_config.api_config))
        
        # Create outputs
        outputs = []
        for output_config in pipeline_config.output:
            if output_config.get('enabled', True):
                output_type = output_config['type'].lower()
                handler_class = cls._output_handlers.get(output_type)
                if not handler_class:
                    raise ValueError(f"Unsupported output type: {output_type}")
                
                output = handler_class(OutputConfig(**output_config))
                outputs.append(output)
        
        # Create and return pipeline
        return Pipeline(
            pipeline_id=pipeline_config.pipeline_id,
            extractor=extractor,
            outputs=outputs
        )

    @staticmethod
    def _import_class(class_path: str) -> Type:
        """Dynamically import a class from its string path."""
        try:
            module_path, class_name = class_path.rsplit('.', 1)
            module = __import__(module_path, fromlist=[class_name])
            return getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            raise ValueError(f"Failed to import {class_path}: {str(e)}")


def load_pipeline_config(pipeline_id: str) -> Dict:
    """Load pipeline configuration from a YAML file in the config directory based on pipeline_id."""
    env = os.getenv('ENVIRONMENT', 'dev')
    config_dir = os.path.join('config', env, 'sources')
    config_path = os.path.join(config_dir, f'{pipeline_id}.yaml')
    
    if not os.path.exists(config_path):
        raise ValueError(f"Pipeline configuration not found: {config_path}")
    
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load pipeline config {config_path}: {str(e)}")
        raise 