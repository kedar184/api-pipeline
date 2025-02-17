# API Pipeline System Design

## Overview

The API Pipeline system is a scalable and extensible framework for extracting data from various APIs and loading it into different output destinations. The system is designed with modularity, flexibility, and reliability in mind.

## Architecture

### High-Level Components

```
┌─────────────────┐     ┌──────────────┐     ┌────────────────┐
│  API Sources    │     │   Pipeline    │     │    Outputs     │
│  (REST/GraphQL) │────▶│   Manager    │────▶│ (BigQuery/GCS) │
└─────────────────┘     └──────────────┘     └────────────────┘
                              │
                        ┌─────┴─────┐
                        │   Config   │
                        │   Store    │
                        └───────────┘
```

### Core Components

1. **Pipeline Factory**
   - Creates pipeline instances
   - Manages component lifecycle
   - Handles configuration
   - Provides component registration

2. **Authentication System**
   - OAuth 2.0 with refresh tokens
   - API key authentication
   - Extensible auth handlers
   - Secure secret management

3. **Extractors**
   - Data source abstraction
   - Concurrent processing
   - Rate limiting
   - Error handling
   - Modular components:
     - Retry handling
     - Watermark tracking
     - Pagination strategies
     - Parallel processing
     - Window management

4. **Outputs**
   - BigQuery integration
   - GCS support
   - Local development output
   - Schema validation
   - Modular output handlers

5. **Pipeline Manager**
   - Orchestration
   - Status tracking
   - Error handling
   - Metrics collection
   - Component coordination

6. **Core Modules**
   - `retry.py`: Retry mechanisms and configurations
   - `watermark.py`: Watermark tracking and window management
   - `pagination.py`: Pagination strategies and configurations
   - `parallel.py`: Parallel processing configurations
   - `windowing.py`: Time window calculations and management
   - `output.py`: Output handling and configurations
   - `auth.py`: Authentication handlers and configurations
   - `base.py`: Core extractor and output base classes

## Core Components Class Diagram

```mermaid
classDiagram
    class PipelineManager {
        +Dict[str, PipelineConfig] pipelines
        +Dict[str, PipelineRun] runs
        +list_pipelines()
        +get_pipeline_status()
        +execute_pipeline()
        +trigger_pipeline()
        +get_run_status()
    }

    class BaseExtractor {
        <<abstract>>
        +ExtractorConfig config
        +RetryHandler retry_handler
        +WatermarkHandler watermark_handler
        +extract(parameters)*
        +get_metrics()
        +cleanup()
        #_process_batch()
        #_paginated_request()
        #_transform_item()*
    }

    class BaseOutput {
        <<abstract>>
        +OutputConfig config
        +write(data)*
        +close()*
        +get_metrics()
        +initialize()*
    }

    class RetryHandler {
        +RetryConfig config
        +RetryState state
        +calculate_delay()
        +should_retry()
        +execute_with_retry()
    }

    class RetryState {
        +Dict[str, Any] _state
        +get_state()
        +clear_state()
        +clear_all()
    }

    class RetryConfig {
        +int max_attempts
        +int max_time
        +float base_delay
        +float max_delay
        +bool jitter
        +List[int] retry_on
    }

    class WatermarkHandler {
        +WatermarkConfig config
        +get_last_watermark()
        +update_watermark()
        +apply_watermark_filters()
        +get_metrics()
    }

    class WatermarkConfig {
        +bool enabled
        +str timestamp_field
        +str watermark_field
        +WindowConfig window
        +datetime initial_watermark
        +str lookback_window
        +str time_format
    }

    class WindowConfig {
        +WindowType window_type
        +str window_size
        +str window_offset
        +str window_overlap
        +str timestamp_field
        +window_size_seconds()
        +window_offset_seconds()
    }

    class PaginationStrategy {
        <<abstract>>
        +get_next_page_params()*
        +get_initial_params()*
    }

    class PaginationConfig {
        +bool enabled
        +PaginationStrategy strategy
        +int page_size
        +int max_pages
        +with_page_numbers()$
        +with_cursor()$
        +with_offset()$
        +with_link_header()$
    }

    class ParallelConfig {
        <<interface>>
    }

    class TimeWindowParallelConfig {
        +str window_size
        +str window_overlap
        +int max_concurrent_windows
        +str timestamp_format
        +window_size_seconds()
        +window_overlap_seconds()
    }

    class KnownPagesParallelConfig {
        +int max_concurrent_pages
        +bool require_total_count
        +int safe_page_size
    }

    class CalculatedOffsetParallelConfig {
        +int max_concurrent_chunks
        +int chunk_size
        +bool require_total_count
    }

    class PipelineConfig {
        +str pipeline_id
        +str description
        +bool enabled
        +str extractor_class
        +Dict api_config
        +List[Dict] output
    }

    class PipelineRun {
        +str run_id
        +str pipeline_id
        +str status
        +datetime start_time
        +datetime end_time
        +int records_processed
        +List[str] errors
    }

    class PipelineFactory {
        +register_output()
        +create_pipeline()
    }

    class ExtractorConfig {
        +str base_url
        +Dict endpoints
        +AuthConfig auth_config
        +ProcessingPattern pattern
        +PaginationConfig pagination
        +RetryConfig retry
        +WatermarkConfig watermark
        +ParallelConfig parallel_config
    }

    class AuthHandler {
        <<abstract>>
        +AuthConfig config
        +get_auth_headers()*
        +get_metrics()
    }

    class AuthConfig {
        +str auth_type
        +Dict auth_credentials
        +str headers_prefix
    }

    class OAuthConfig {
        +str token_url
        +str client_id
        +str client_secret
        +str refresh_token
        +str access_token
        +datetime token_expiry
        +bool auto_refresh
    }

    PipelineManager --> PipelineConfig : manages
    PipelineManager --> PipelineRun : tracks
    PipelineManager --> PipelineFactory : uses
    PipelineFactory --> BaseExtractor : creates
    PipelineFactory --> BaseOutput : creates
    BaseExtractor --> RetryHandler : uses
    BaseExtractor --> WatermarkHandler : uses
    BaseExtractor --> PaginationStrategy : uses
    BaseExtractor --> AuthHandler : uses
    BaseExtractor --> ExtractorConfig : configured by
    BaseExtractor <|-- WeatherExtractor : implements
    BaseExtractor <|-- GitHubExtractor : implements
    BaseOutput <|-- BigQueryOutput : implements
    BaseOutput <|-- GCSOutput : implements
    RetryHandler --> RetryConfig : configured by
    RetryHandler --> RetryState : manages
    WatermarkHandler --> WatermarkConfig : configured by
    WatermarkConfig --> WindowConfig : uses
    PaginationStrategy <|-- PageNumberStrategy : implements
    PaginationStrategy <|-- CursorStrategy : implements
    PaginationStrategy <|-- LinkHeaderStrategy : implements
    PaginationStrategy <|-- OffsetStrategy : implements
    AuthHandler <|-- OAuthHandler : implements
    AuthHandler <|-- BearerAuth : implements
    AuthHandler <|-- ApiKeyAuth : implements
    AuthHandler --> AuthConfig : configured by
    OAuthHandler --> OAuthConfig : configured by
    ParallelConfig <|-- TimeWindowParallelConfig : implements
    ParallelConfig <|-- KnownPagesParallelConfig : implements
    ParallelConfig <|-- CalculatedOffsetParallelConfig : implements
    ExtractorConfig --> ParallelConfig : uses
    ExtractorConfig --> PaginationConfig : uses
    PaginationConfig --> PaginationStrategy : uses
```

## Core Components Interaction

### 1. Base Framework (`base.py`)
- Provides abstract base classes `BaseExtractor` and `BaseOutput`
- Defines core extraction and output interfaces
- Coordinates component interactions
- Manages component lifecycle
- Delegates specialized functionality to dedicated modules

### 2. Retry System (`retry.py`)
- Handles request retries with configurable strategies
- Manages retry state and backoff calculations
- Provides retry metrics and monitoring
- Integrates with extractors for automatic retry handling

### 3. Watermark System (`watermark.py`)
- Tracks data processing progress
- Manages incremental data extraction
- Handles time-based windowing
- Provides watermark metrics and monitoring

### 4. Pagination System (`pagination.py`)
- Implements various pagination strategies
- Manages page state and continuation
- Handles response parsing and validation
- Supports dynamic strategy selection

### 5. Parallel Processing (`parallel.py`)
- Configures parallel execution strategies
- Manages concurrent request handling
- Optimizes resource utilization
- Provides performance monitoring

### 6. Window Management (`windowing.py`)
- Calculates time-based windows
- Manages window boundaries and overlap
- Supports different window types
- Coordinates with watermark tracking

### 7. Output System (`output.py`)
- Manages output destinations
- Handles data writing and validation
- Provides output metrics and monitoring
- Supports multiple output formats

### 8. Authentication (`auth.py`)
- Manages authentication methods
- Handles token refresh and rotation
- Provides secure credential management
- Supports multiple auth strategies

### 9. Data Models (`models.py`)
- Defines core data structures
- Ensures type safety and validation
- Provides configuration models
- Supports extensible schemas

### 10. Factory Pattern (`factory.py`)
- Creates component instances
- Manages component registration
- Handles configuration parsing
- Enables plug-and-play architecture

### 11. Pipeline Manager (`pipeline_manager.py`)
- Orchestrates component interactions
- Manages pipeline execution
- Tracks pipeline status
- Provides monitoring interface

## Extensibility Points

1. **Adding New Data Sources**
   ```python
   class NewAPIExtractor(BaseExtractor):
       async def extract(self, parameters):
           # Implementation for new API
   ```

2. **Adding New Outputs**
   ```python
   class NewOutput(BaseOutput):
       async def write(self, data):
           # Implementation for new destination
   ```

3. **Registering New Components**
   ```python
   PipelineFactory.register_output("new_type", NewOutput)
   ```

## Flow of Control

1. **Pipeline Creation**
   - `PipelineManager` loads configurations
   - `PipelineFactory` creates components:
     - Initializes auth handler
     - Creates retry handler
     - Sets up watermark handler
     - Configures pagination strategy
     - Prepares parallel processing
     - Initializes output handlers
   - Components are initialized with configs

2. **Pipeline Execution**
   - Manager triggers execution
   - Watermark handler determines processing windows
   - For each window:
     - Parallel processor manages concurrent execution
     - Extractor fetches data with pagination
     - Retry handler manages failed requests
     - Data flows through transforms
   - Outputs write to destinations
   - Watermark is updated

3. **Component Interaction**
   - Base extractor coordinates components
   - Retry handler wraps HTTP requests
   - Watermark handler tracks progress
   - Pagination handles request continuation
   - Window manager calculates boundaries
   - Output handlers manage destinations

4. **Status Tracking**
   - Run status is updated
   - Component metrics are collected:
     - Retry statistics
     - Watermark progress
     - Pagination metrics
     - Processing windows
     - Output statistics
   - Errors are captured and categorized

## Design Benefits

1. **Loose Coupling**
   - Components interact through well-defined interfaces
   - Implementation details are isolated in dedicated modules
   - Easy to modify or replace individual components
   - Clear separation between core and specialized functionality

2. **High Cohesion**
   - Each module has a single, focused responsibility
   - Clear separation of concerns in dedicated files
   - Easy to maintain and test individual components
   - Simplified debugging and error tracking

3. **Extensibility**
   - New extractors can be added without modifying core
   - New outputs can be integrated seamlessly
   - Custom components can be easily plugged in
   - Configurations are flexible and modular

4. **Modularity**
   - Specialized functionality in dedicated modules
   - Clear interface boundaries between components
   - Dependency injection for flexible configuration
   - Factory pattern for component creation
   - Easy to add new features in isolation

5. **Maintainability**
   - Clear organization of code by functionality
   - Comprehensive documentation per module
   - Type hints and validation throughout
   - Consistent coding standards
   - Isolated testing of components

6. **Reusability**
   - Common functionality extracted to shared modules
   - Standardized interfaces across components
   - Configurable components for different use cases
   - Easy to share code between extractors

## Design Principles

1. **Modularity**
   - Pluggable architecture
   - Clear interface boundaries
   - Dependency injection
   - Factory pattern for components

2. **Reliability**
   - Error handling at all levels
   - Retry mechanisms
   - Comprehensive logging
   - Transaction management

3. **Scalability**
   - Asynchronous operations
   - Batch processing
   - Resource pooling
   - Configurable concurrency

4. **Maintainability**
   - Clear separation of concerns
   - Comprehensive documentation
   - Type hints and validation
   - Consistent coding standards

## Data Flow

1. **Authentication**
   - Load auth configuration
   - Initialize auth handler
   - Manage token lifecycle
   - Secure header generation

2. **Extraction**
   - Authenticate requests
   - Fetch data from source
   - Handle pagination
   - Transform data

3. **Processing**
   - Validate data
   - Apply transformations
   - Batch processing
   - Error handling

4. **Output**
   - Write to destinations
   - Schema enforcement
   - Partitioning
   - Cleanup

## Configuration Management

### Environment-Specific Config
```yaml
service:
  name: api-pipeline
  version: 1.0.0
  log_level: INFO

rate_limits:
  default_rps: 500
  max_concurrent: 200
```

### Pipeline Config
```yaml
pipeline_id: weather_api
extractor_class: WeatherExtractor
output:
  - type: bigquery
    config:
      dataset_id: raw_data
      table_id: weather
  - type: gcs
    enabled: true
    config:
      bucket: data-lake
```

## Security

1. **Authentication**
   - OAuth 2.0 support
   - Automatic token refresh
   - Secure credential storage
   - Environment isolation

2. **Secret Management**
   - Google Cloud Secret Manager
   - Environment-specific secrets
   - Credential rotation
   - Access control

3. **Access Control**
   - IAM integration
   - Service accounts
   - Minimal permissions
   - Audit logging

## Monitoring and Observability

1. **Metrics**
   - Pipeline success rates
   - Data volume metrics
   - Processing latency
   - Resource utilization

2. **Logging**
   - Structured logging
   - Error tracking
   - Audit trails
   - Performance monitoring

3. **Alerting**
   - Pipeline failures
   - Rate limit warnings
   - Resource exhaustion
   - Data quality issues

## Future Enhancements

1. **Planned Features**
   - Schema evolution handling
   - Real-time processing
   - Data quality checks
   - Advanced monitoring

2. **Scalability Improvements**
   - Distributed processing
   - Dynamic scaling
   - Cache optimization
   - Performance tuning

3. **Integration Options**
   - Additional data sources
   - New output formats
   - Workflow integration
   - API gateway support

## Batch Processing and Data Flow

### Batch Processing Architecture

```mermaid
sequenceDiagram
    participant API as API Source
    participant Extractor
    participant Pipeline
    participant Output
    participant Storage as Storage (GCS/BQ)

    Note over Extractor: Batch Size: 100 (default)
    loop API Batches
        Extractor->>API: Fetch Batch
        API-->>Extractor: Return Data
        Extractor->>Extractor: Transform Data
    end
    Extractor->>Pipeline: Complete Dataset
    Note over Pipeline: Combines All Batches
    Pipeline->>Output: Full Dataset
    Note over Output: Batch Size: 1000 (default)
    loop Output Batches
        Output->>Storage: Write Batch
    end
```

### Multi-Level Batching

1. **Extractor Level Batching**
   - Default batch size: 100 records
   - Purpose: Optimize API calls and memory usage
   - Process:
     ```python
     async def extract(self):
         all_data = []
         for batch in batches:
             batch_data = await self._process_batch(batch)
             all_data.extend(batch_data)
         return all_data  # Complete dataset
     ```

2. **Pipeline Level**
   - Acts as a data aggregator
   - Collects all extractor batches
   - Passes complete dataset to outputs
   - No batching at this level

3. **Output Level Batching**
   - Default batch size: 1000 records (GCS example)
   - Purpose: Optimize storage writes
   - Process:
     ```python
     async def write(self, data):
         for record in data:
             current_batch.append(record)
             if len(current_batch) >= batch_size:
                 await self._write_batch()
     ```

### Example Data Flow

For a dataset of 2500 records:

1. **Extraction Phase**
   - 25 API batches of 100 records each
   - Each batch processed concurrently (up to max_concurrent_requests)
   - All batches combined into single dataset

2. **Pipeline Phase**
   - Receives complete dataset (2500 records)
   - Passes to configured outputs

3. **Output Phase (GCS Example)**
   - Breaks 2500 records into output batches
   - Creates 3 files:
     - File 1: 1000 records
     - File 2: 1000 records
     - File 3: 500 records

### Batch Configuration

```yaml
api_config:
  # Extractor batch settings
  batch_size: 100
  max_concurrent_requests: 10
  rate_limit: 60

output:
  - type: "gcs"
    config:
      # Output batch settings
      batch_size: 1000
      file_format: "jsonl"
```

### Benefits of Multi-Level Batching

1. **Performance Optimization**
   - Efficient API data fetching
   - Controlled memory usage
   - Optimized storage writes

2. **Resource Management**
   - Rate limiting at API level
   - Controlled concurrent requests
   - Efficient file management

3. **Reliability**
   - Independent batch error handling
   - Partial success capability
   - Clear error tracking per batch 

## Parallel Processing and Window Management

### Window-Based Processing Architecture

```mermaid
graph TD
    A[Time Range] --> B[Window Manager]
    B --> C1[Window 1]
    B --> C2[Window 2]
    B --> C3[Window 3]
    C1 --> D[Parallel Processor]
    C2 --> D
    C3 --> D
    D --> E1[Batch 1]
    D --> E2[Batch 2]
    D --> E3[Batch 3]
    E1 --> F[Output Manager]
    E2 --> F
    E3 --> F
```

### Window Processing Strategy

1. **Time Window Division**
   - Large time ranges split into fixed windows
   - Default window size: 24 hours
   - Configurable via `window_size` parameter
   ```python
   window_config = WindowConfig(
       window_type=WindowType.FIXED,
       window_size="24h",
       window_offset="0m"
   )
   ```

2. **Parallel Window Processing**
   - Concurrent processing of multiple windows
   - Controlled by `max_concurrent_requests`
   - Semaphore-based concurrency control
   ```python
   async def _process_windows(self, windows):
       semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
       async with semaphore:
           return await asyncio.gather(*[self._process_window(w) for w in windows])
   ```

3. **Window-Level Watermarking**
   - Track progress per window
   - Prevent duplicate processing
   - Enable incremental updates
   ```python
   async def _update_window_watermark(self, window_end):
       self._watermark_store[self._get_watermark_key()] = window_end
   ```

### Enhanced Batch Processing

1. **Multi-Level Batching**
   - API-level batching (configurable size)
   - Window-level batching (time-based)
   - Output-level batching (storage optimization)

2. **Batch Size Optimization**
   ```python
   # Extractor level
   batch_size = min(
       self.config.batch_size,
       self.config.rate_limit // self.config.max_concurrent_requests
   )
   
   # Output level
   output_batch_size = min(
       self.config.output_batch_size,
       available_memory // estimated_record_size
   )
   ```

3. **Memory Management**
   - Controlled batch loading
   - Immediate batch processing
   - Efficient resource cleanup
   ```python
   async def _process_batch(self, batch):
       try:
           result = await self._transform_batch(batch)
           await self._write_batch(result)
       finally:
           # Clean up batch resources
           del batch
   ```

### Performance Monitoring

1. **Window Metrics**
   ```python
   window_metrics = {
       'window_start': window.start_time,
       'window_end': window.end_time,
       'processing_time': processing_duration,
       'records_processed': record_count,
       'batch_count': batch_count
   }
   ```

2. **Batch Performance**
   ```python
   batch_metrics = {
       'batch_size': len(batch),
       'processing_time': batch_duration,
       'memory_usage': current_memory,
       'success_rate': successful_records / total_records
   }
   ```

3. **Resource Utilization**
   - Memory usage per batch
   - Processing time per window
   - Concurrent operations count
   - I/O operations monitoring

### Error Handling and Recovery

1. **Window-Level Recovery**
   ```python
   async def _process_window_with_retry(self, window):
       for attempt in range(max_retries):
           try:
               return await self._process_window(window)
           except Exception as e:
               if attempt == max_retries - 1:
                   raise
               await asyncio.sleep(retry_delay)
   ```

2. **Batch-Level Error Handling**
   ```python
   async def _process_batch_safe(self, batch):
       try:
           return await self._process_batch(batch)
       except Exception as e:
           logger.error(f"Batch processing failed: {str(e)}")
           self._metrics['failed_batches'] += 1
           return []
   ```

3. **Partial Success Handling**
   - Track failed records
   - Continue processing valid records
   - Report detailed error metrics 