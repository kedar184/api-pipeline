from abc import ABC, abstractmethod
from datetime import datetime, timedelta, UTC
from typing import Dict, Optional, Union, ClassVar, Any
import aiohttp
from pydantic import BaseModel, Field
from loguru import logger

from api_pipeline.core.types import AuthType

class AuthConfig(BaseModel):
    """Base authentication configuration."""
    auth_type: str
    auth_credentials: Dict[str, str]
    headers_prefix: Optional[str] = None  # Custom prefix for auth headers (e.g., "Token" instead of "Bearer")

class OAuthConfig(AuthConfig):
    """OAuth specific configuration."""
    token_url: str
    client_id: str
    client_secret: str
    refresh_token: str
    access_token: Optional[str] = None
    token_expiry: Optional[datetime] = None
    scope: Optional[str] = None
    auto_refresh: bool = True  # Whether to automatically refresh expired tokens
    refresh_margin: int = 300  # Seconds before expiry to trigger refresh

class BasicAuthConfig(AuthConfig):
    """Basic auth configuration."""
    username: str
    password: str
    use_base64: bool = True  # Whether to base64 encode credentials

class BaseAuth(ABC):
    """Base authentication handler."""
    
    def __init__(self):
        self._metrics = {
            'auth_attempts': 0,
            'auth_failures': 0,
            'last_auth_time': None,
            'token_refreshes': 0
        }
    
    @abstractmethod
    async def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for request."""
        pass
    
    def get_metrics(self) -> Dict[str, any]:
        """Get authentication metrics."""
        return self._metrics.copy()

class OAuthHandler(BaseAuth):
    """OAuth authentication with refresh token support."""
    
    def __init__(self, config: OAuthConfig):
        super().__init__()
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _ensure_session(self):
        """Ensure aiohttp session exists."""
        if not self._session:
            self._session = aiohttp.ClientSession()

    async def _refresh_token(self):
        """Refresh the access token using refresh token."""
        try:
            self._metrics['auth_attempts'] += 1
            await self._ensure_session()
            
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.config.refresh_token,
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret
            }
            if self.config.scope:
                data["scope"] = self.config.scope
            
            logger.info("Refreshing OAuth token...")
            async with self._session.post(self.config.token_url, data=data) as response:
                response.raise_for_status()
                token_data = await response.json()
                
                self.config.access_token = token_data["access_token"]
                # Some providers also refresh the refresh token
                if "refresh_token" in token_data:
                    self.config.refresh_token = token_data["refresh_token"]
                
                # Set token expiry (default to 1 hour if not provided)
                expires_in = token_data.get("expires_in", 3600)
                self.config.token_expiry = datetime.now(UTC) + timedelta(seconds=expires_in)
                
                self._metrics['token_refreshes'] += 1
                self._metrics['last_auth_time'] = datetime.now(UTC)
                logger.info("Successfully refreshed OAuth token")
        
        except Exception as e:
            self._metrics['auth_failures'] += 1
            logger.error(f"Failed to refresh OAuth token: {str(e)}")
            raise ValueError(f"Failed to refresh OAuth token: {str(e)}")
        
        finally:
            if self._session:
                await self._session.close()
                self._session = None

    def _should_refresh(self) -> bool:
        """Determine if token needs refresh based on expiry and margin."""
        if not self.config.auto_refresh:
            return False
            
        if not self.config.access_token or not self.config.token_expiry:
            return True
            
        now = datetime.now(UTC)
        refresh_time = self.config.token_expiry - timedelta(seconds=self.config.refresh_margin)
        return now >= refresh_time

    async def get_auth_headers(self) -> Dict[str, str]:
        """Get OAuth headers, refreshing token if needed."""
        if self._should_refresh():
            await self._refresh_token()
        
        return {
            "Authorization": f"Bearer {self.config.access_token}"
        }

class ApiKeyAuth:
    """API Key authentication handler that supports both header and query parameter auth."""
    def __init__(self, auth_config: AuthConfig):
        """Initialize API Key auth handler.
        
        Args:
            auth_config: Authentication configuration
        """
        self.auth_config = auth_config
        if 'api_key' not in auth_config.auth_credentials:
            raise ValueError("API key not found in credentials")
        self.api_key = auth_config.auth_credentials['api_key']
        self.headers_prefix = auth_config.headers_prefix

    async def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers.
        
        Returns:
            Dictionary of auth headers
        """
        if self.headers_prefix:
            return {'Authorization': f"{self.headers_prefix} {self.api_key}"}
        return {}

    def get_auth_params(self) -> Dict[str, str]:
        """Get authentication query parameters.
        
        Returns:
            Dictionary of auth parameters
        """
        return {'appid': self.api_key}

    def get_metrics(self) -> Dict[str, Any]:
        """Get authentication metrics.
        
        Returns:
            Dictionary of metrics
        """
        return {
            'auth_type': 'api_key',
            'has_prefix': bool(self.headers_prefix)
        }

class BearerAuth(BaseAuth):
    """Bearer token authentication."""
    
    def __init__(self, config: AuthConfig):
        super().__init__()
        self.config = config
    
    async def get_auth_headers(self) -> Dict[str, str]:
        """Get bearer token headers."""
        self._metrics['auth_attempts'] += 1
        self._metrics['last_auth_time'] = datetime.now(UTC)
        
        prefix = self.config.headers_prefix or "Bearer"
        return {
            "Authorization": f"{prefix} {self.config.auth_credentials['token']}"
        }

class BasicAuth(BaseAuth):
    """Basic authentication."""
    
    def __init__(self, config: BasicAuthConfig):
        super().__init__()
        self.config = config
    
    async def get_auth_headers(self) -> Dict[str, str]:
        """Get basic auth headers."""
        self._metrics['auth_attempts'] += 1
        self._metrics['last_auth_time'] = datetime.now(UTC)
        
        if self.config.use_base64:
            import base64
            credentials = f"{self.config.username}:{self.config.password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            return {"Authorization": f"Basic {encoded}"}
        
        return {
            "Authorization": f"Basic {self.config.username}:{self.config.password}"
        }

def create_auth_handler(config: Union[AuthConfig, OAuthConfig, BasicAuthConfig]) -> BaseAuth:
    """Factory function to create appropriate auth handler.
    
    Args:
        config: Authentication configuration
        
    Returns:
        Appropriate authentication handler instance
        
    Raises:
        ValueError: If auth type is not supported
    """
    handlers = {
        AuthType.OAUTH: OAuthHandler,
        AuthType.API_KEY: ApiKeyAuth,
        AuthType.BEARER: BearerAuth,
        AuthType.BASIC: BasicAuth
    }
    
    handler_class = handlers.get(config.auth_type.lower())
    if not handler_class:
        raise ValueError(
            f"Unsupported auth type: {config.auth_type}. "
            f"Supported types: {', '.join(handlers.keys())}"
        )
    
    logger.info(f"Creating auth handler for type: {config.auth_type}")
    return handler_class(config) 