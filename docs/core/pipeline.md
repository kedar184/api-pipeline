# Pipeline Core

The pipeline module contains the core execution logic for data pipelines, handling the flow of data from extractors to outputs.

## Core Components

### Pipeline

Main pipeline class that orchestrates data extraction and output writing.

#### Constructor
```python
def __init__(
    pipeline_id: str,
    extractor: BaseExtractor,
    outputs: List[BaseOutput]
)
```

#### Methods

- `async execute(config: PipelineRunConfig) -> PipelineRun`
  - Executes the pipeline with given configuration
  - Manages the complete pipeline lifecycle
  - Handles errors and resource cleanup
  - Returns detailed run information

#### Execution Flow
1. Generate unique run ID
2. Initialize run status
3. Extract data using configured extractor
4. Write data to all configured outputs
5. Update run status and metrics
6. Clean up resources
7. Return run details

### PipelineRunConfig

Configuration model for a single pipeline execution.

#### Attributes
- `pipeline_id`: Unique identifier for the pipeline
- `parameters`: Optional parameters for the run
- `output_config`: Optional output configuration overrides

### PipelineRun

Model representing a single pipeline execution instance.

#### Attributes
- `run_id`: Unique identifier for the run
- `pipeline_id`: Identifier of the pipeline
- `status`: Current execution status
- `start_time`: Run start time
- `end_time`: Run completion time
- `parameters`: Run parameters
- `records_processed`: Count of processed records
- `errors`: List of encountered errors

## Example Usage

```python
# Create pipeline instance
pipeline = Pipeline(
    pipeline_id="weather_api",
    extractor=weather_extractor,
    outputs=[bigquery_output, gcs_output]
)

# Execute pipeline
config = PipelineRunConfig(
    pipeline_id="weather_api",
    parameters={"location_ids": ["NYC", "SF"]}
)
run = await pipeline.execute(config)
```

## Error Handling

The pipeline implements comprehensive error handling:

1. **Extraction Errors**
   - Captures and logs extraction failures
   - Updates run status with error details
   - Prevents output writing on extraction failure

2. **Output Errors**
   - Handles individual output failures
   - Continues with remaining outputs on error
   - Records errors in run status

3. **Resource Cleanup**
   - Ensures output handlers are properly closed
   - Handles cleanup errors gracefully
   - Prevents resource leaks

## Design Benefits

1. **Asynchronous Execution**
   - Non-blocking pipeline execution
   - Efficient resource utilization
   - Better scalability

2. **Flexible Configuration**
   - Runtime parameter configuration
   - Output configuration overrides
   - Environment-specific settings

3. **Comprehensive Monitoring**
   - Detailed run status tracking
   - Error capture and reporting
   - Performance metrics collection 