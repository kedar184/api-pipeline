# API Pipeline Framework

A flexible framework for building data pipelines that extract data from APIs with features like watermark-based incremental loading, batching, and multiple output formats.

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/yourusername/api-pipeline.git
cd api-pipeline
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up your environment variables in `.env`:
```bash
# Environment
ENVIRONMENT=local

# API Keys
LOCAL_GITHUB_TOKEN=your_github_personal_access_token_here

# Local Development Settings
LOG_LEVEL=DEBUG
CONFIG_PATH=config/local/config.yaml
```

A high-performance, scalable framework for ingesting data from various API sources into BigQuery and other destinations. This framework provides a configuration-driven approach to set up and manage multiple data ingestion pipelines with built-in performance optimizations.

## Features

- Configuration-driven pipeline setup
- Built-in performance optimizations:
  - Concurrent request processing
  - Automatic batching
  - Connection pooling
  - Rate limiting
  - Efficient resource management
- Support for multiple API authentication methods (Basic, Bearer Token, API Key)
- Automatic schema creation in BigQuery
- Flexible pagination handling
- FastAPI-based monitoring and control interface
- Extensible architecture for custom extractors and outputs
- Robust error handling and logging

## Documentation

Detailed documentation is available in the `docs` directory:

- [System Design](docs/system_design.md) - Architecture and core components
- Core Components:
  - [Base Classes](docs/core/base.md) - Foundational classes and interfaces
  - [Models](docs/core/models.md) - Data models and validation
  - [Factory](docs/core/factory.md) - Component creation and management
  - [Pipeline](docs/core/pipeline.md) - Pipeline execution and lifecycle
  - [Pipeline Manager](docs/core/pipeline_manager.md) - Orchestration and monitoring
  - [Secrets Management](docs/core/secrets.md) - Secure credential handling

## Project Structure

```
api-pipeline/
├── .cloud/                  # Cloud-related configurations
│   └── ci/                 # CI/CD configurations
│       ├── cloudbuild.yaml        # Production build
│       ├── cloudbuild-dev.yaml    # Development build
│       └── cloudbuild-prod.yaml   # Production-specific build
├── src/
│   └── api_pipeline/       # Main package
├── tests/                  # Test suite
├── docs/                   # Documentation
└── config/                 # Pipeline configurations
    ├── dev/               # Development environment
    ├── prod/              # Production environment
    └── local/             # Local development
```

## Example Implementations

The framework includes two example extractors demonstrating different API integration patterns:

1. **GitHub Extractor** (`extractors/github.py`)
   - Demonstrates API chaining (repositories → issues)
   - Handles pagination
   - OAuth authentication
   - Data transformation

2. **Weather Extractor** (`extractors/weather.py`)
   - Shows simple API integration
   - API key authentication
   - Batch processing
   - Error handling

## Configuration

Pipelines are configured using YAML files in the `config` directory. Each pipeline requires:

1. API Configuration
   - Base URL
   - Endpoints
   - Authentication details
   - Rate limiting settings
   - Pagination settings
   - Performance tuning:
     - Batch size
     - Concurrent requests
     - Timeouts

2. Output Configuration
   - Output type (BigQuery, GCS, etc.)
   - Connection details
   - Schema definition
   - Write disposition

Example configuration:
```yaml
pipeline_id: weather_api
description: "Ingests weather data from OpenWeatherMap API"
enabled: true
extractor_class: "api_pipeline.extractors.weather.WeatherExtractor"
api_config:
  base_url: "https://api.openweathermap.org/data/2.5"
  endpoints:
    current: "/weather"
  auth_type: "api_key"
  auth_credentials:
    api_key: "${secret:projects/my-project/secrets/weather-api-key/versions/latest}"
  batch_size: 100
  max_concurrent_requests: 10
  session_timeout: 30
  pagination:
    enabled: true
    page_size: 50
output:
  - type: "bigquery"
    config:
      project_id: "${GCP_PROJECT_ID}"
      dataset_id: "raw_data"
      table_id: "weather_data"
      schema:
        - name: "timestamp"
          type: "TIMESTAMP"
        - name: "location_id"
          type: "STRING"
        - name: "temperature"
          type: "FLOAT"
```

## Environment Variables

Create a `.env` file with the following variables:
```
GCP_PROJECT_ID=your-project-id
WEATHER_API_KEY=your-api-key
GITHUB_TOKEN=your-github-token
```

## Usage

### Starting the API Server

```bash
uvicorn api_pipeline.main:app --reload
```

### API Endpoints

- `GET /pipelines`: List all configured pipelines
- `GET /pipelines/{pipeline_id}`: Get pipeline status
- `POST /pipelines/{pipeline_id}/trigger`: Trigger a pipeline run
- `GET /pipelines/{pipeline_id}/runs/{run_id}`: Get run status

### Creating a Custom Extractor

1. Create a new class that inherits from `BaseExtractor`
2. Implement the required methods:
   - `_transform_item()`: Transform individual items
   - `_get_item_params()`: Convert items to API parameters (optional)
   - `_ensure_session()`: Configure API session (optional)

Example:
```python
class CustomExtractor(BaseExtractor):
    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": item["id"],
            "name": item["name"],
            "timestamp": datetime.now(UTC)
        }
```

## Development

### Running Tests

```bash
pytest
```

### Code Style

This project follows PEP 8 guidelines. Format your code using:
```bash
black .
```

## CI/CD

The project uses Google Cloud Build for CI/CD, with configurations in `.cloud/ci/`:

1. **Development Build** (cloudbuild-dev.yaml):
   - Runs tests with coverage
   - Performs linting and type checking
   - Builds and deploys to dev environment
   - Enables unauthenticated access for testing

2. **Production Build** (cloudbuild-prod.yaml):
   - Runs security scanning
   - Builds with production optimizations
   - Deploys with strict security settings
   - Requires authentication
   - Uses VPC connector

3. **Common Features**:
   - Automated testing
   - Container building
   - Artifact versioning
   - Cloud Run deployment
   - Secret management

## Example: GitHub Commits Extractor

The GitHub commits extractor demonstrates the framework's watermark-based incremental loading capabilities.

### Basic Usage

1. Extract commits from a repository:
```bash
python -m api_pipeline run --pipeline github_commits --repo="apache/spark"
```

2. Extract commits with branch filter:
```bash
python -m api_pipeline run --pipeline github_commits \
  --repo="apache/airflow" \
  --branch="main"
```

3. Extract commits by specific author:
```bash
python -m api_pipeline run --pipeline github_commits \
  --repo="apache/kafka" \
  --author="johndoe"
```

### Watermark-Based Extraction

The extractor uses watermarks to track processed commits and enable incremental loading:

```yaml
# config/local/sources/github_commits.yaml
watermark:
  enabled: true
  timestamp_field: "committed_at"
  window:
    window_type: "fixed"
    window_size: "6h"      # Process in 6-hour windows
    window_offset: "0m"
  lookback_window: "1d"    # Look back 1 day on first run
```

This configuration:
- Processes commits in 6-hour windows
- Tracks the last processed commit timestamp
- Only fetches new commits in subsequent runs
- Looks back 1 day on first run