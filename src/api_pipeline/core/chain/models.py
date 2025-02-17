from enum import Enum
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field
from api_pipeline.core.base import ExtractorConfig

class ProcessingMode(str, Enum):
    SINGLE = "single"      # Process single items directly
    SEQUENTIAL = "sequential"  # Process lists sequentially
    PARALLEL = "parallel"  # Process lists in parallel

class ErrorHandlingMode(str, Enum):
    FAIL_FAST = "fail_fast"    # Stop on first error
    CONTINUE = "continue"      # Skip errors and continue
    RETRY = "retry"           # Retry failed items

class ResultMode(str, Enum):
    LIST = "list"    # Flat list of results
    DICT = "dict"    # Results grouped by input key

class RetryConfig(BaseModel):
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0

class SequentialConfig(BaseModel):
    error_handling: ErrorHandlingMode = ErrorHandlingMode.CONTINUE
    result_handling: ResultMode = ResultMode.DICT
    batch_size: Optional[int] = None

class ParallelConfig(BaseModel):
    max_concurrent: int = 5
    batch_size: Optional[int] = None
    error_handling: ErrorHandlingMode = ErrorHandlingMode.CONTINUE
    result_handling: ResultMode = ResultMode.DICT
    timeout_per_item: Optional[int] = None
    retry: Optional[RetryConfig] = None

class ChainedExtractorConfig(BaseModel):
    """Configuration for a single extractor in a chain."""
    extractor_class: str              # Fully qualified class path
    extractor_config: Dict[str, Any]  # Base extractor configuration
    input_mapping: Dict[str, str]     # Maps extractor params to chain state
    output_mapping: Dict[str, str]    # Maps extractor output to chain state
    processing_mode: ProcessingMode = ProcessingMode.SINGLE
    sequential_config: Optional[SequentialConfig] = None
    parallel_config: Optional[ParallelConfig] = None

class ChainConfig(BaseModel):
    """Configuration for an entire extraction chain."""
    chain_id: str                      # Unique identifier for the chain
    extractors: List[ChainedExtractorConfig]  # List of extractor configurations
    max_parallel_chains: int = 1       # Maximum parallel chain executions
    state_dir: str                     # Directory for state management 