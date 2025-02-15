# Authentication System

The authentication system provides a flexible and secure way to handle different authentication methods for API access. It supports OAuth 2.0 with refresh token capabilities and API key authentication.

## Components

### Configuration Models

#### AuthConfig
Base configuration for all authentication methods.
```python
class AuthConfig(BaseModel):
    auth_type: str
    auth_credentials: Dict[str, str]
```

#### OAuthConfig
Extended configuration for OAuth authentication.
```python
class OAuthConfig(AuthConfig):
    token_url: str
    client_id: str
    client_secret: str
    refresh_token: str
    access_token: Optional[str] = None
    token_expiry: Optional[datetime] = None
    scope: Optional[str] = None
```

### Authentication Handlers

#### BaseAuth
Abstract base class defining the authentication interface.
```python
class BaseAuth(ABC):
    @abstractmethod
    async def get_auth_headers(self) -> Dict[str, str]:
        pass
```

#### OAuthHandler
OAuth 2.0 implementation with refresh token support.

Features:
- Automatic token refresh
- Token expiry management
- Refresh token rotation
- Scope support
- Async HTTP operations
- Session management

#### ApiKeyAuth
Simple API key authentication implementation.

## Usage Examples

### OAuth Configuration
```yaml
api_config:
  auth_type: "oauth"
  auth_credentials:
    client_id: "${secret:oauth_client_id}"
    client_secret: "${secret:oauth_client_secret}"
    refresh_token: "${secret:oauth_refresh_token}"
  token_url: "https://api.service.com/oauth/token"
  scope: "read write"
```

### API Key Configuration
```yaml
api_config:
  auth_type: "api_key"
  auth_credentials:
    api_key: "${secret:api_key}"
```

### Using the Factory
```python
auth_config = AuthConfig(...)
auth_handler = create_auth_handler(auth_config)
headers = await auth_handler.get_auth_headers()
```

## Security Best Practices

1. **Secret Management**
   - Store credentials in Secret Manager
   - Use environment-specific secrets
   - Rotate credentials regularly

2. **Token Handling**
   - Never log token values
   - Clear tokens from memory when possible
   - Use secure token storage

3. **Error Handling**
   - Handle token refresh failures gracefully
   - Implement retry mechanisms
   - Log authentication errors securely

## Implementation Details

### OAuth Token Refresh
The OAuth handler automatically:
1. Tracks token expiry
2. Refreshes before expiration
3. Updates refresh tokens if provided
4. Maintains session efficiency

### API Key Management
The API key handler:
1. Provides consistent header format
2. Supports multiple header formats
3. Validates key presence

## Configuration Examples

### GitHub API (OAuth)
```yaml
auth_config:
  auth_type: "oauth"
  token_url: "https://github.com/login/oauth/access_token"
  client_id: "${secret:github_client_id}"
  client_secret: "${secret:github_client_secret}"
  refresh_token: "${secret:github_refresh_token}"
  scope: "repo read:org"
```

### Weather API (API Key)
```yaml
auth_config:
  auth_type: "api_key"
  auth_credentials:
    api_key: "${secret:weather_api_key}"
```

## Error Handling

The authentication system handles various error scenarios:

1. **Token Refresh Failures**
   - Network errors
   - Invalid credentials
   - Rate limiting
   - Service unavailability

2. **Configuration Errors**
   - Missing credentials
   - Invalid auth type
   - Malformed configuration

3. **Runtime Errors**
   - Session management issues
   - Token storage problems
   - Concurrent refresh attempts

## Extending the System

To add a new authentication method:

1. Create a configuration class (if needed)
```python
class NewAuthConfig(AuthConfig):
    # Add new configuration fields
    pass
```

2. Implement the handler
```python
class NewAuthHandler(BaseAuth):
    async def get_auth_headers(self) -> Dict[str, str]:
        # Implement authentication logic
        pass
```

3. Register in factory
```python
handlers = {
    "oauth": OAuthHandler,
    "api_key": ApiKeyAuth,
    "new_auth": NewAuthHandler
}
``` 