from enum import Enum
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field
from api_pipeline.core.base import ExtractorConfig

class ProcessingMode(str, Enum):
    """Processing modes for extractors"""
    SINGLE = "single"       # Process single item
    SEQUENTIAL = "sequential"  # Process list items one by one
    PARALLEL = "parallel"   # Process list items concurrently

class ErrorHandlingMode(str, Enum):
    """Error handling modes"""
    FAIL_FAST = "fail_fast"  # Stop on first error
    CONTINUE = "continue"    # Skip errors and continue

class ResultMode(str, Enum):
    """Result handling modes"""
    LIST = "list"  # Return list of results
    DICT = "dict"  # Return dict of results

class RetryConfig(BaseModel):
    """Configuration for retries"""
    max_retries: int = 3
    delay_seconds: int = 1
    backoff_factor: float = 2.0

class SequentialConfig(BaseModel):
    """Configuration for sequential processing"""
    error_handling: ErrorHandlingMode = ErrorHandlingMode.FAIL_FAST
    result_handling: ResultMode = ResultMode.LIST

class ParallelConfig(BaseModel):
    """Configuration for parallel processing"""
    max_concurrent: int = 5
    batch_size: Optional[int] = None
    error_handling: ErrorHandlingMode = ErrorHandlingMode.FAIL_FAST
    result_handling: ResultMode = ResultMode.LIST

class ChainedExtractorConfig(BaseModel):
    """Configuration for a single extractor in a chain"""
    extractor_class: str              # Fully qualified class path
    extractor_config: Dict[str, Any]  # Base extractor configuration
    input_mapping: Dict[str, str]     # Maps chain state to extractor params
    output_mapping: Dict[str, str]    # Maps extractor output to chain state
    required: bool = True             # Whether this extractor is required
    processing_mode: ProcessingMode = ProcessingMode.SINGLE
    sequential_config: Optional[SequentialConfig] = None
    parallel_config: Optional[ParallelConfig] = None
    custom_config: Optional[Dict[str, Any]] = None  # Extractor-specific custom configuration

class ConditionConfig(BaseModel):
    """Configuration for a condition"""
    field: str                         # Field to check
    operator: str                      # Operator to use (gt, lt, eq)
    value: Any                         # Value to compare against
    type: Optional[str] = None         # Type of condition (e.g., 'threshold', 'status', etc.)

class ChainStep(BaseModel):
    """Configuration for a step in the chain"""
    extractor: str                     # Name of the extractor to use
    condition: Optional[Dict[str, Any]] = None  # Condition for running this step
    output_mapping: List[Dict[str, str]]  # List of output mappings

class ChainConfig(BaseModel):
    """Configuration for an entire extraction chain"""
    name: str                          # Name of the chain
    description: Optional[str] = None   # Description of the chain
    chain_id: str                      # Unique identifier for the chain
    extractors: Dict[str, ChainedExtractorConfig]  # Map of extractor name to config
    chain: List[ChainStep]             # List of steps in the chain
    max_parallel_chains: int = 1       # Maximum parallel chain executions
    state_dir: str                     # Directory for state management
    base_config: Optional[Dict[str, Any]] = None  # Base configuration for all extractors

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
            config.update(self.base_config)
        
        # Add extractor-specific configuration
        extractor = self.extractors[extractor_name]
        config.update(extractor.extractor_config)
        
        return config 