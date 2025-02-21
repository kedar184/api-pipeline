# Chain Executor

The Chain Executor is a powerful component that manages the execution of extractor chains with configurable processing modes. It handles sequential and parallel execution of data extraction steps while managing state and error handling.

## Core Components

### ChainExecutor

The main class responsible for executing chains of extractors with different processing modes and configurations.

```python
class ChainExecutor:
    def __init__(self):
        self._metrics = {
            'chains_executed': 0,
            'chains_failed': 0,
            'total_execution_time': 0.0,
            'last_execution_time': None,
            'items_processed': 0,
            'errors_encountered': 0,
            'retries_performed': 0
        }
```

### Processing Modes

The executor supports three processing modes:

```python
class ProcessingMode(str, Enum):
    SINGLE = "single"       # Process single item
    SEQUENTIAL = "sequential"  # Process list items one by one
    PARALLEL = "parallel"   # Process list items concurrently
```

### Configuration Models

#### ChainConfig
Configuration for a chain of extractors:

```python
class ChainConfig(BaseModel):
    chain_id: str
    extractors: List[ChainedExtractorConfig]
    state_dir: Optional[str] = None
    max_parallel_chains: int = 5
```

#### ChainedExtractorConfig
Configuration for each extractor in the chain:

```python
class ChainedExtractorConfig(BaseModel):
    extractor_class: str
    extractor_config: Dict[str, Any]
    processing_mode: ProcessingMode = ProcessingMode.SINGLE
    input_mapping: Dict[str, str]
    output_mapping: Dict[str, str]
    sequential_config: Optional[SequentialConfig] = None
    parallel_config: Optional[ParallelConfig] = None
```

## Usage Examples

### Single Mode Execution
```python
chain_config = ChainConfig(
    chain_id="user_repos_chain",
    extractors=[
        ChainedExtractorConfig(
            extractor_class="api_pipeline.extractors.github.GithubUserExtractor",
            extractor_config={"api_key": "xxx"},
            processing_mode=ProcessingMode.SINGLE,
            input_mapping={"username": "github_user"},
            output_mapping={"repositories": "user_repos"}
        )
    ]
)

executor = ChainExecutor()
results = await executor.execute_single_chain(
    chain_config,
    {"github_user": "octocat"}
)
```

### Sequential Processing
```python
chain_config = ChainConfig(
    chain_id="repo_commits_chain",
    extractors=[
        ChainedExtractorConfig(
            extractor_class="api_pipeline.extractors.github.GithubCommitsExtractor",
            extractor_config={"api_key": "xxx"},
            processing_mode=ProcessingMode.SEQUENTIAL,
            input_mapping={"repository": "repository_list"},
            output_mapping={"commits": "all_commits"},
            sequential_config=SequentialConfig(
                error_handling=ErrorHandlingMode.CONTINUE,
                result_handling=ResultMode.LIST
            )
        )
    ]
)

executor = ChainExecutor()
results = await executor.execute_single_chain(
    chain_config,
    {"repository_list": ["org/repo1", "org/repo2"]}
)
```

### Parallel Processing
```python
chain_config = ChainConfig(
    chain_id="weather_chain",
    extractors=[
        ChainedExtractorConfig(
            extractor_class="api_pipeline.extractors.weather.WeatherExtractor",
            extractor_config={"api_key": "xxx"},
            processing_mode=ProcessingMode.PARALLEL,
            input_mapping={"location": "cities"},
            output_mapping={"temperature": "temperatures"},
            parallel_config=ParallelConfig(
                max_concurrent=5,
                batch_size=10,
                error_handling=ErrorHandlingMode.CONTINUE,
                result_handling=ResultMode.DICT
            )
        )
    ]
)

executor = ChainExecutor()
results = await executor.execute_single_chain(
    chain_config,
    {"cities": ["London", "New York", "Tokyo"]}
)
```

## Error Handling

The Chain Executor provides robust error handling through configurable modes:

```python
class ErrorHandlingMode(str, Enum):
    FAIL_FAST = "fail_fast"   # Stop on first error
    CONTINUE = "continue"     # Skip errors and continue
```

Errors are captured and stored in the chain state with the "_errors" key. Each error includes:
- Item identifier
- Error message
- Timestamp
- Step information

## Metrics

The executor tracks various metrics during execution:
- Number of chains executed/failed
- Total execution time
- Average execution time
- Items processed
- Errors encountered
- Success rate
- Error rate

Metrics can be accessed using the `get_metrics()` method:
```python
metrics = executor.get_metrics()
print(f"Success rate: {metrics['success_rate']:.2%}")
```

## State Management

The Chain Executor uses a state manager to maintain the state between steps:
- Each chain has its own isolated state
- State is persisted to disk (optional)
- Automatic cleanup after chain completion
- Thread-safe state access

## Best Practices

1. **Chain Design**
   - Keep chains focused and single-purpose
   - Use appropriate processing modes for the data structure
   - Configure error handling based on business requirements

2. **Performance**
   - Use parallel processing for independent operations
   - Configure appropriate batch sizes and concurrency limits
   - Monitor metrics to optimize performance

3. **Error Handling**
   - Use FAIL_FAST for critical operations
   - Use CONTINUE for non-critical bulk operations
   - Always check the "_errors" key in results

4. **State Management**
   - Clean up state after chain completion
   - Use meaningful state keys
   - Avoid storing large objects in state