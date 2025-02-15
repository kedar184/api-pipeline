# Pipeline Manager

The `PipelineManager` is the core orchestrator of the API Pipeline system, responsible for managing and executing data pipelines.

## Responsibilities

1. Loading and managing pipeline configurations from YAML files
2. Dynamically loading extractor classes based on configuration
3. Executing pipelines with proper error handling and status tracking
4. Managing pipeline runs and their lifecycle
5. Providing status and monitoring capabilities

## Key Features

- **Asynchronous Pipeline Execution**: Pipelines run asynchronously to prevent blocking
- **Pipeline Run Status Tracking**: Monitor the status and progress of pipeline runs
- **Configurable Data Extractors**: Dynamic loading of data extractors based on configuration
- **Flexible Output Handlers**: Support for multiple output destinations (BigQuery, GCS)
- **Error Handling and Logging**: Comprehensive error tracking and logging
- **Run History and Filtering**: Query and filter pipeline run history

## Usage Example

```python
manager = PipelineManager()

# List available pipelines
pipelines = manager.list_pipelines()

# Trigger a pipeline
result = await manager.trigger_pipeline("weather_api", {"location": "NYC"})

# Get run status
status = manager.get_run_status("weather_api", result["run_id"])
```

## API Reference

### Pipeline Management

- `list_pipelines() -> List[Dict[str, Any]]`
  - Lists all configured pipelines with their metadata
  - Returns: List of pipeline configurations

- `get_pipeline_status(pipeline_id: str) -> Dict[str, Any]`
  - Gets the current status of a specific pipeline
  - Returns: Pipeline status including last run details

### Pipeline Execution

- `trigger_pipeline(pipeline_id: str, params: Optional[Dict] = None) -> Dict[str, str]`
  - Triggers asynchronous execution of a pipeline
  - Returns: Run ID and initial status

- `execute_pipeline(pipeline_id: str, run_id: str, params: Optional[Dict] = None) -> None`
  - Internal method for pipeline execution
  - Handles data extraction, transformation, and output

### Run Management

- `list_pipeline_runs(pipeline_id: str, limit: int = 10, status: Optional[str] = None, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[Dict[str, Any]]`
  - Lists pipeline runs with optional filtering
  - Supports filtering by status and time range

- `get_run_status(pipeline_id: str, run_id: str) -> Dict[str, Any]`
  - Gets detailed status of a specific pipeline run
  - Returns: Run details including status, timing, and errors 