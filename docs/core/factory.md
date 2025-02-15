# Pipeline Factory

The `PipelineFactory` is responsible for creating pipeline instances and managing output handler registration. It implements a factory pattern to dynamically create pipeline components based on configuration.

## Core Components

### PipelineFactory

Static factory class that manages pipeline creation and output handler registration.

#### Class Methods

- `register_output(type_name: str, handler_class: Type[BaseOutput]) -> None`
  - Registers an output handler type for use in pipelines
  - Used by output implementations to make themselves available
  - Case-insensitive type name matching

- `create_pipeline(config: Dict) -> Pipeline`
  - Creates a pipeline instance from configuration
  - Validates configuration using `PipelineConfig` model
  - Creates extractor and output instances
  - Returns configured `Pipeline` instance

#### Example Usage
```python
# Register an output handler
PipelineFactory.register_output("bigquery", BigQueryOutput)

# Create a pipeline from config
config = {
    "pipeline_id": "weather_api",
    "extractor_class": "api_pipeline.extractors.WeatherExtractor",
    "api_config": {...},
    "output": [
        {
            "type": "bigquery",
            "config": {...}
        }
    ]
}
pipeline = PipelineFactory.create_pipeline(config)
```

### Configuration Loading

The module also provides configuration loading functionality:

#### Functions

- `load_pipeline_config(pipeline_id: str) -> Dict`
  - Loads pipeline configuration from YAML file
  - Environment-aware (dev/prod) configuration loading
  - Validates file existence and YAML structure
  - Returns raw configuration dictionary

#### Configuration Structure
```yaml
pipeline_id: example_pipeline
description: "Example pipeline configuration"
enabled: true
extractor_class: "package.module.ExtractorClass"
api_config:
  base_url: "https://api.example.com"
  auth_type: "bearer"
output:
  - type: "bigquery"
    config:
      dataset_id: "raw_data"
      table_id: "example_table"
```

## Design Benefits

1. **Extensibility**
   - Easy registration of new output handlers
   - Pluggable architecture for components
   - Configuration-driven pipeline creation

2. **Separation of Concerns**
   - Factory handles component creation
   - Components focus on their specific responsibilities
   - Clear separation between configuration and implementation

3. **Validation**
   - Configuration validation through Pydantic models
   - Type checking of components
   - Early error detection 