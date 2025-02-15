# Pipeline Models

Core data models for pipeline configuration and execution. These models provide type safety, validation, and structured data representation throughout the pipeline system.

## Configuration Models

### PipelineParameter

Definition of a configurable pipeline parameter.

#### Attributes
- `name`: Name of the parameter
- `type`: Data type (string, integer, array, etc.)
- `required`: Whether the parameter is required (defaults to False)
- `description`: Description of the parameter
- `default`: Default value if not provided

#### Validation
- Type must be one of: string, integer, float, boolean, array, object
- Type is case-insensitive
- Name must be a valid identifier

### PipelineConfig

Configuration for a complete data pipeline.

#### Attributes
- `pipeline_id`: Unique identifier for the pipeline
- `description`: Description of what the pipeline does
- `enabled`: Whether the pipeline is enabled (defaults to True)
- `extractor_class`: Fully qualified class name of the extractor
- `api_config`: Configuration for the API extractor
- `parameters`: List of available parameters
- `output`: Output configurations

#### Validation
- `pipeline_id`: Only letters, numbers, underscores, and hyphens
- `extractor_class`: Valid Python import path
- `parameters`: List of valid PipelineParameter objects
- `output`: At least one output configuration required

## Execution Models

### PipelineRunStatus vs PipelineStatus

Two distinct status models serving different purposes:

#### PipelineRunStatus
Internal model for detailed execution tracking.

##### Attributes
- `status`: Current status of the run
- `message`: Additional status message
- `timestamp`: Status recording time (UTC)

##### Validation
- Status must be one of: pending, running, completed, failed, cancelled
- Timestamp is always UTC timezone-aware

#### PipelineStatus
External model for pipeline health monitoring.

##### Attributes
- `pipeline_id`: Pipeline identifier
- `status`: Current pipeline status
- `last_run`: Last execution timestamp
- `records_processed`: Processing metrics
- `errors`: Error collection

##### Usage
- API response model
- Monitoring interface
- Dashboard displays

### PipelineRun

Complete execution record model.

#### Attributes
- `run_id`: Unique run identifier
- `pipeline_id`: Pipeline identifier
- `status`: Execution status
- `start_time`: Start timestamp (UTC)
- `end_time`: End timestamp (UTC)
- `parameters`: Execution parameters
- `records_processed`: Processing count
- `errors`: Error collection
- `statuses`: Status change history

#### Validation
- Status must be one of: pending, running, completed, failed, cancelled
- End time must be after start time
- All timestamps are UTC timezone-aware

### PipelineRunRequest

API request model for pipeline execution.

#### Attributes
- `parameters`: Optional execution parameters
- `output_config`: Optional output overrides

#### Validation
- Parameters must match pipeline configuration
- Output overrides must be valid for output type

## Example Usage

```python
# Pipeline configuration
config = PipelineConfig(
    pipeline_id="weather_api",
    description="Weather data ingestion pipeline",
    enabled=True,
    extractor_class="api_pipeline.extractors.weather.WeatherExtractor",
    api_config={
        "base_url": "https://api.weather.com",
        "auth_type": "api_key"
    },
    parameters=[
        PipelineParameter(
            name="location",
            type="string",
            required=True,
            description="Location ID to fetch weather for"
        )
    ]
)

# Execution tracking
run = PipelineRun(
    run_id="run_123",
    pipeline_id="weather_api",
    status="running",
    start_time=datetime.now(UTC)
)

# Status update
status = PipelineRunStatus(
    status="completed",
    message="Successfully processed weather data",
    timestamp=datetime.now(UTC)
)
``` 