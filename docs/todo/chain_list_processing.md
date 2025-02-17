# Chain List Processing Design

## Core Principle: Single Responsibility Extractors

### Base Extractor Responsibilities
- Each base extractor handles a single, well-defined entity
- Relies on base framework for:
  - Pagination
  - Authentication
  - Rate limiting
  - Retry logic
  - Watermarking

Example: `GitHubCommitsExtractor`
```python
class GitHubCommitsExtractor(BaseExtractor):
    """Extracts commits for a single repository."""
    
    async def extract(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Single responsibility: Get commits for one repo
        repo = parameters["repo"]
        return await self._get_commits_for_repo(repo)
```

## Chain Processing Modes

### 1. Single Item Processing (Default)
- Direct pass-through of parameters and results
- No list handling or parallel execution
- Simple sequential processing

Example:
```python
# Single item processing
result = await github_commits_extractor.extract({"repo": "apache/spark"})
# Returns: List of commits for single repo
```

### 2. List Processing (Optional)
When enabled via configuration:
- Takes output list from previous step
- Processes each item through next extractor
- Aggregates results

Example Configuration:
```python
chain_config = {
    "steps": [
        # Step 1: Get list of repositories
        {
            "extractor": GitHubReposExtractor(),
            "input": {"org": "organization"},
            "output": "repos",
            "processing_mode": ProcessingMode.SINGLE  # Returns list of repos
        },
        # Step 2: Process repos sequentially
        {
            "extractor": GitHubCommitsExtractor(),
            "input": {"repo": "repos"},  # Takes list from previous step
            "output": "commits_by_repo",
            "processing_mode": ProcessingMode.SEQUENTIAL,  # Process list sequentially
            "sequential_config": {
                "error_handling": ErrorHandlingMode.CONTINUE,  # Continue on individual failures
                "result_handling": ResultMode.DICT,  # Group results by repo
                "batch_size": None  # Process one at a time
            }
        }
    ]
}

# Results structure for sequential processing
{
    "repos": ["apache/spark", "apache/airflow"],
    "commits_by_repo": {
        "apache/spark": [...commits...],
        "apache/airflow": [...commits...]
    }
}
```

### 3. Parallel Execution (Optional)
When enabled via configuration:
- Concurrent processing of list items
- Resource utilization control
- Error handling per item

Example Configuration:
```python
chain_config = {
    "steps": [
        # Step 1: Get list of repositories
        {
            "extractor": GitHubReposExtractor(),
            "input": {"org": "organization"},
            "output": "repos",
            "processing_mode": ProcessingMode.SINGLE
        },
        # Step 2: Process repos in parallel
        {
            "extractor": GitHubCommitsExtractor(),
            "input": {"repo": "repos"},
            "output": "commits_by_repo",
            "processing_mode": ProcessingMode.PARALLEL,
            "parallel_config": {
                "max_concurrent": 5,  # Maximum concurrent extractions
                "batch_size": 10,     # Process in batches of 10
                "error_handling": ErrorHandlingMode.CONTINUE,
                "result_handling": ResultMode.DICT,
                "timeout_per_item": 30,  # Timeout in seconds
                "retry": {
                    "max_attempts": 3,
                    "base_delay": 1.0
                }
            }
        }
    ]
}

# Results structure for parallel processing
{
    "repos": ["apache/spark", "apache/airflow"],
    "commits_by_repo": {
        "apache/spark": [...commits...],
        "apache/airflow": [...commits...],
        "_errors": {  # Optional error tracking
            "apache/flink": {
                "error": "Timeout after 30 seconds",
                "attempts": 3
            }
        }
    }
}
```

### Configuration Models
```python
class ProcessingMode(Enum):
    SINGLE = "single"
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"

class ErrorHandlingMode(Enum):
    FAIL_FAST = "fail_fast"    # Stop on first error
    CONTINUE = "continue"      # Skip errors and continue
    RETRY = "retry"           # Retry failed items

class ResultMode(Enum):
    LIST = "list"    # Flat list of results
    DICT = "dict"    # Results grouped by input key

class SequentialConfig:
    error_handling: ErrorHandlingMode = ErrorHandlingMode.CONTINUE
    result_handling: ResultMode = ResultMode.DICT
    batch_size: Optional[int] = None

class ParallelConfig:
    max_concurrent: int = 5
    batch_size: Optional[int] = None
    error_handling: ErrorHandlingMode = ErrorHandlingMode.CONTINUE
    result_handling: ResultMode = ResultMode.DICT
    timeout_per_item: Optional[int] = None
    retry: Optional[RetryConfig] = None

class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
```

### Key Configuration Differences

1. **Sequential Processing**
   - Simpler configuration
   - Focus on error handling and result format
   - Optional batching for memory management
   - Predictable ordering of results

2. **Parallel Processing**
   - Rich configuration options
   - Resource control (max_concurrent)
   - Sophisticated error handling
   - Timeout and retry mechanisms
   - Batch processing support

## Implementation Strategy

### 1. Chain Step Configuration
```python
class ChainStepConfig:
    extractor: BaseExtractor
    input_mapping: Dict[str, str]
    output_mapping: Dict[str, str]
    processing_mode: ProcessingMode = ProcessingMode.SINGLE  # Default to single
    parallel_config: Optional[ParallelConfig] = None  # Only for parallel mode
```

### 2. Processing Mode Configuration
```python
class ProcessingMode(Enum):
    SINGLE = "single"      # Process single items directly
    SEQUENTIAL = "sequential"  # Process lists sequentially
    PARALLEL = "parallel"  # Process lists in parallel

class ParallelConfig:
    max_concurrent: int = 5
    batch_size: Optional[int] = None
    error_handling: ErrorHandlingMode = ErrorHandlingMode.CONTINUE
```

### 3. Chain Executor Enhancement
```python
class ChainExecutor:
    async def execute_step(
        self,
        step_config: ChainStepConfig,
        input_data: Any
    ) -> Any:
        """Execute step based on configured mode."""
        if step_config.processing_mode == ProcessingMode.SINGLE:
            return await self._execute_single(step_config, input_data)
            
        elif step_config.processing_mode == ProcessingMode.SEQUENTIAL:
            return await self._execute_sequential(step_config, input_data)
            
        elif step_config.processing_mode == ProcessingMode.PARALLEL:
            return await self._execute_parallel(
                step_config,
                input_data,
                step_config.parallel_config
            )

    async def _execute_single(self, config: ChainStepConfig, data: Any) -> Any:
        """Process single item directly."""
        params = self._map_input(data, config.input_mapping)
        return await config.extractor.extract(params)

    async def _execute_sequential(self, config: ChainStepConfig, items: List[Any]) -> List[Any]:
        """Process list items sequentially."""
        results = []
        for item in items:
            result = await self._execute_single(config, item)
            results.append(result)
        return results

    async def _execute_parallel(
        self,
        config: ChainStepConfig,
        items: List[Any],
        parallel_config: ParallelConfig
    ) -> List[Any]:
        """Process list items in parallel with configuration."""
        semaphore = asyncio.Semaphore(parallel_config.max_concurrent)
        
        async def process_with_semaphore(item):
            async with semaphore:
                try:
                    return await self._execute_single(config, item)
                except Exception as e:
                    if parallel_config.error_handling == ErrorHandlingMode.CONTINUE:
                        logger.error(f"Error processing item {item}: {str(e)}")
                        return None
                    raise
        
        results = await asyncio.gather(*[
            process_with_semaphore(item) for item in items
        ])
        return [r for r in results if r is not None]  # Filter out errors if continuing
```

## Example: GitHub Data Chain

```python
# Configuration with mixed processing modes
chain_config = {
    "steps": [
        {
            "extractor": GitHubReposExtractor(),
            "input": {"org": "organization"},
            "output": "repos",
            "processing_mode": ProcessingMode.SINGLE  # Single org -> list of repos
        },
        {
            "extractor": GitHubCommitsExtractor(),
            "input": {"repo": "repos"},
            "output": "commits_by_repo",
            "processing_mode": ProcessingMode.PARALLEL,  # Process repos in parallel
            "parallel_config": {
                "max_concurrent": 5,
                "error_handling": ErrorHandlingMode.CONTINUE
            }
        }
    ]
}

# Execution remains the same
executor = ChainExecutor()
results = await executor.execute(chain_config, {"organization": "apache"})
```

## Benefits of Optional Processing Modes

1. **Flexibility**
   - Choose appropriate mode per step
   - Simple single-item processing when needed
   - Parallel processing for performance when required

2. **Resource Control**
   - Parallel execution only when configured
   - Fine-grained control over concurrency
   - Better resource utilization

3. **Error Handling Options**
   - Different strategies for different modes
   - Partial success handling in parallel mode
   - Simple error propagation in single mode

4. **Performance Optimization**
   - Avoid overhead of list processing when unnecessary
   - Scale up parallel processing where beneficial
   - Mix modes for optimal performance

## Next Steps

1. Implement processing mode configuration
2. Add mode-specific execution logic
3. Enhance error handling per mode
4. Add mode-specific metrics
5. Create examples of mixed-mode chains 