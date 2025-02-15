# Secrets Management

The secrets module provides access to secrets stored in Google Cloud Secret Manager.

## Core Components

### SecretManager

Handles retrieval of secrets from Google Cloud Secret Manager.

#### Initialization
```python
secret_manager = SecretManager(project_id="my-project")  # or uses GOOGLE_CLOUD_PROJECT env var
```

#### Methods

- `get_secret(secret_path: str) -> str`
  - Retrieves a secret value using its full path
  - Implements caching to reduce API calls
  - Example: `get_secret("projects/my-project/secrets/weather-api-key/versions/latest")`

## Usage Examples

### Basic Usage
```python
from api_pipeline.core.secrets import SecretManager

# Initialize
secret_manager = SecretManager()

# Retrieve a secret
api_key = secret_manager.get_secret("projects/my-project/secrets/weather-api-key/versions/latest")
```

### In Pipeline Configuration
```yaml
api_config:
  base_url: "https://api.weather.com"
  auth_credentials:
    api_key: "${secret:projects/my-project/secrets/weather-api-key/versions/latest}"
```

The factory will automatically resolve any secret references in the configuration using the format:
```
${secret:projects/PROJECT_ID/secrets/SECRET_ID/versions/VERSION}
```

## Best Practices

1. **Secret Management**
   - Use Infrastructure as Code (IaC) tools to manage secret lifecycle
   - Follow your organization's security policies for secret rotation

2. **Version Management**
   - Use explicit versions for critical systems
   - Default to "latest" for frequently rotated secrets

3. **Error Handling**
   - Handle secret access failures gracefully
   - Provide clear error messages
   - Implement retry logic for transient failures

4. **Security**
   - Never log secret values
   - Clear secrets from memory when possible
   - Use minimal IAM permissions

## Secret Organization

Recommended secret hierarchy:
```
api_pipeline/
├── weather/
│   ├── api_key
│   └── client_secret
├── github/
│   ├── oauth_token
│   └── webhook_secret
├── bigquery/
│   └── service_account
└── gcs/
    └── credentials
``` 