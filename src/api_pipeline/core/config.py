from typing import Dict, Any, List, Optional, TypeVar, Generic, Type
from pydantic import BaseModel, Field
import yaml
from loguru import logger
import os

from api_pipeline.core.chain.models import (
    ChainedExtractorConfig,
    ProcessingMode,
    ChainStep,
    ConditionConfig
)
from api_pipeline.core.types import ProcessingPattern

class AuthConfig(BaseModel):
    """Authentication configuration"""
    type: str
    env_var: Optional[str] = None
    auth_credentials: Optional[Dict[str, str]] = None
    headers_prefix: Optional[str] = None

class BaseConfig(BaseModel):
    """Base configuration for extractors"""
    base_url: str
    rate_limits: Optional[Dict[str, int]] = None  # Different types of rate limits
    timeout: Optional[int] = None
    retry_config: Optional[Dict[str, Any]] = None

class EndpointConfig(BaseModel):
    """Endpoint configuration"""
    endpoints: Dict[str, str]
    parameters: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None

class ExtractorConfig(BaseModel):
    """Individual extractor configuration"""
    type: str
    config: EndpointConfig
    enabled: bool = True
    required: bool = True
    custom_config: Optional[Dict[str, Any]] = None  # Extractor-specific configuration

class OutputMapping(BaseModel):
    """Output mapping configuration"""
    source: str
    target: str
    transform: Optional[str] = None  # Optional transformation function name

class ConditionConfig(BaseModel):
    """Condition configuration"""
    field: str
    operator: str
    value: Any
    type: Optional[str] = None  # Type of condition (e.g., 'threshold', 'status', etc.)

T = TypeVar('T', bound=BaseModel)

class ChainConfig(BaseModel, Generic[T]):
    """Configuration for an entire extraction chain"""
    name: str                          # Name of the chain
    description: Optional[str] = None   # Description of the chain
    chain_id: str                      # Unique identifier for the chain
    extractors: Dict[str, ChainedExtractorConfig]  # Map of extractor name to config
    chain: List[ChainStep]             # List of steps in the chain
    max_parallel_chains: int = 1       # Maximum parallel chain executions
    state_dir: str                     # Directory for state management
    custom_config: Optional[T] = None  # Type-safe custom configuration
    base_config: Optional[BaseConfig] = None  # Base configuration for all extractors
    
    @classmethod
    def from_yaml(cls: Type['ChainConfig[T]'], file_path: str, custom_config_class: Optional[Type[T]] = None) -> 'ChainConfig[T]':
        """Load configuration from YAML file.
        
        Args:
            file_path: Path to YAML configuration file
            custom_config_class: Optional class for scenario-specific configuration
            
        Returns:
            ChainConfig instance with optional custom configuration
        """
        logger.info(f"Loading configuration from {file_path}")
        try:
            with open(file_path, 'r') as f:
                config_dict = yaml.safe_load(f)
            
            # Handle custom configuration if provided
            if custom_config_class and 'custom_config' in config_dict:
                config_dict['custom_config'] = custom_config_class(
                    **config_dict['custom_config']
                )
            
            # Convert extractor configurations
            if 'extractors' in config_dict:
                extractors = {}
                for name, extractor_config in config_dict['extractors'].items():
                    # Handle extractor-specific custom config
                    custom_config = extractor_config.get('custom_config')
                    
                    # Map ProcessingPattern to ProcessingMode
                    extractor_type = extractor_config.get('type', 'SINGLE').upper()
                    processing_mode = ProcessingMode.SINGLE
                    if extractor_type == ProcessingPattern.BATCH.value:
                        processing_mode = ProcessingMode.SEQUENTIAL
                    
                    # Convert extractor config to ChainedExtractorConfig
                    extractors[name] = ChainedExtractorConfig(
                        extractor_class=extractor_config['extractor_class'],
                        extractor_config=extractor_config.get('config', {}),
                        input_mapping=extractor_config['input_mapping'],
                        output_mapping=extractor_config['output_mapping'],
                        required=extractor_config.get('required', True),
                        processing_mode=processing_mode,
                        custom_config=custom_config
                    )
                config_dict['extractors'] = extractors
            
            # Convert chain steps
            if 'chain' in config_dict:
                chain = []
                for step in config_dict['chain']:
                    # Convert condition if present
                    condition = None
                    if step.get('condition'):
                        condition = step['condition']  # Keep as dictionary
                    
                    # Convert step to ChainStep
                    chain.append(ChainStep(
                        extractor=step['extractor'],
                        condition=condition,
                        output_mapping=step['output_mapping']
                    ))
                config_dict['chain'] = chain
            
            return cls(**config_dict)
            
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise

    def get_extractor_config(self, extractor_name: str) -> Dict[str, Any]:
        """Get configuration for a specific extractor.
        
        Args:
            extractor_name: Name of the extractor
            
        Returns:
            Combined configuration from base_config and extractor-specific config
            
        Raises:
            ValueError: If extractor not found in configuration
        """
        if extractor_name not in self.extractors:
            raise ValueError(f"Extractor {extractor_name} not found in configuration")
        
        # Start with base configuration if available
        config = {}
        if self.base_config:
            config = self.base_config.model_dump(exclude_none=True)
        
        # Add extractor-specific configuration
        extractor = self.extractors[extractor_name]
        config.update(extractor.extractor_config)
        
        # Add custom configuration if present
        if extractor.custom_config:
            config['custom_config'] = extractor.custom_config
        
        return config

    def get_chain_step(self, extractor_name: str) -> Optional[ChainStep]:
        """Get chain step configuration for a specific extractor.
        
        Args:
            extractor_name: Name of the extractor
            
        Returns:
            ChainStep configuration if found, None otherwise
        """
        for step in self.chain:
            if step.extractor == extractor_name:
                return step
        return None

    def validate_chain(self) -> List[str]:
        """Validate the chain configuration.
        
        Returns:
            List of validation errors, empty if valid
        """
        errors = []
        
        # Check for required extractors
        required_extractors = {
            name for name, config in self.extractors.items() 
            if config.required
        }
        
        chain_extractors = {step.extractor for step in self.chain}
        missing_required = required_extractors - chain_extractors
        
        if missing_required:
            errors.append(
                f"Required extractors missing from chain: {', '.join(missing_required)}"
            )
        
        # Validate extractor dependencies
        for step in self.chain:
            if step.condition:
                field = step.condition.get('field')
                if not field:
                    errors.append(
                        f"Missing field in condition for extractor '{step.extractor}'"
                    )
                else:
                    # Check if field comes from previous steps
                    field_found = any(
                        field in mapping['target']
                        for prev_step in self.chain[:self.chain.index(step)]
                        for mapping in prev_step.output_mapping
                    )
                    if not field_found:
                        errors.append(
                            f"Condition field '{field}' for extractor '{step.extractor}' "
                            "not found in previous steps"
                        )
        
        return errors 