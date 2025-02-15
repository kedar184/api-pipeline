from abc import ABC, abstractmethod
from datetime import datetime, timedelta, UTC
from typing import Dict, Optional
import aiohttp
from pydantic import BaseModel

class AuthConfig(BaseModel):
    """Base authentication configuration."""
    auth_type: str
    auth_credentials: Dict[str, str]

class OAuthConfig(AuthConfig):
    """OAuth specific configuration."""
    token_url: str
    client_id: str
    client_secret: str
    refresh_token: str
    access_token: Optional[str] = None
    token_expiry: Optional[datetime] = None
    scope: Optional[str] = None

class BaseAuth(ABC):
    """Base authentication handler."""
    
    @abstractmethod
    async def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for request."""
        pass

class OAuthHandler(BaseAuth):
    """OAuth authentication with refresh token support."""
    
    def __init__(self, config: OAuthConfig):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _ensure_session(self):
        """Ensure aiohttp session exists."""
        if not self._session:
            self._session = aiohttp.ClientSession()

    async def _refresh_token(self):
        """Refresh the access token using refresh token."""
        try:
            await self._ensure_session()
            
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.config.refresh_token,
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret
            }
            if self.config.scope:
                data["scope"] = self.config.scope
            
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
        
        except Exception as e:
            raise ValueError(f"Failed to refresh OAuth token: {str(e)}")
        
        finally:
            if self._session:
                await self._session.close()
                self._session = None

    async def get_auth_headers(self) -> Dict[str, str]:
        """Get OAuth headers, refreshing token if needed."""
        # Check if token needs refresh
        if (not self.config.access_token or 
            not self.config.token_expiry or 
            datetime.now(UTC) >= self.config.token_expiry):
            await self._refresh_token()
        
        return {
            "Authorization": f"Bearer {self.config.access_token}"
        }

class ApiKeyAuth(BaseAuth):
    """API Key authentication."""
    
    def __init__(self, config: AuthConfig):
        self.config = config
    
    async def get_auth_headers(self) -> Dict[str, str]:
        """Get API key headers."""
        return {
            "Authorization": f"ApiKey {self.config.auth_credentials['api_key']}"
        }

class BearerAuth(BaseAuth):
    """Bearer token authentication."""
    
    def __init__(self, config: AuthConfig):
        self.config = config
    
    async def get_auth_headers(self) -> Dict[str, str]:
        """Get bearer token headers."""
        return {
            "Authorization": f"Bearer {self.config.auth_credentials['token']}"
        }

def create_auth_handler(config: AuthConfig) -> BaseAuth:
    """Factory function to create appropriate auth handler."""
    handlers = {
        "oauth": OAuthHandler,
        "api_key": ApiKeyAuth,
        "bearer": BearerAuth
    }
    
    handler_class = handlers.get(config.auth_type.lower())
    if not handler_class:
        raise ValueError(f"Unsupported auth type: {config.auth_type}")
    
    return handler_class(config) 