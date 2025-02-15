# API Pipeline Configuration

This directory contains environment-specific configurations for the API Pipeline system.

## Directory Structure

```
config/
├── dev/                    # Development environment
│   ├── config.yaml        # Dev environment config
│   └── sources/           # Dev source configurations
│       ├── weather_api.yaml
│       └── github_api.yaml
├── prod/                   # Production environment
│   ├── config.yaml        # Prod environment config
│   └── sources/           # Prod source configurations
│       ├── weather_api.yaml
│       └── github_api.yaml
└── README.md              # This file
```

## Environment-Specific Configuration

Each environment (dev, prod) has its own:
- API endpoints and credentials
- Rate limits and retry settings
- Output configurations (BigQuery datasets, GCS buckets)
- Environment-specific features (e.g., additional logging in dev)

## Source Configurations

Source configurations define:
- Pipeline metadata (ID, description)
- API connection details
- Authentication settings
- Available parameters
- Output schemas and destinations

## Environment Variables

The following environment variables must be set:
- `WEATHER_API_KEY_DEV` / `WEATHER_API_KEY_PROD`
- `GITHUB_TOKEN_DEV` / `GITHUB_TOKEN_PROD`
- `GCP_PROJECT_ID_DEV` / `GCP_PROJECT_ID_PROD`
- `GCS_BUCKET_DEV` / `GCS_BUCKET_PROD`

## Key Differences Between Environments

Development:
- Lower rate limits
- Additional logging
- GCS output enabled for debugging
- Shorter data retention

Production:
- Higher rate limits
- Minimal logging
- BigQuery-only output
- Longer data retention
- VPC access restrictions

## Output Handlers

The pipeline system supports multiple output handlers:

### BigQuery Output
The primary output handler for structured data. Configured in source YAML files under the `output` section.
Note: The legacy `BigQueryLoader` has been deprecated in favor of the new `BigQueryOutput` handler, which provides:
- Better error handling
- Async operations
- Partitioning support
- Schema validation
- Retry logic

### GCS Output
Optional output handler for raw data storage, typically enabled in development for debugging.

