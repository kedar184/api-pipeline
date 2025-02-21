from typing import Optional, Dict
from functools import lru_cache
import os

from google.cloud import secretmanager
from loguru import logger


class SecretManager:
    """Handles secret retrieval from Google Cloud Secret Manager."""
    
    def __init__(self, project_id: Optional[str] = None, test_mode: bool = False):
        self.test_mode = test_mode
        if not test_mode:
            self.project_id = project_id or os.getenv('GOOGLE_CLOUD_PROJECT')
            if not self.project_id:
                raise ValueError("Project ID must be provided or set in GOOGLE_CLOUD_PROJECT environment variable")
            self.client = secretmanager.SecretManagerServiceClient()
        else:
            self.test_secrets: Dict[str, str] = {}
    
    @lru_cache(maxsize=100)
    def get_secret(self, secret_path: str) -> str:
        """
        Get a secret value from Secret Manager using its full path.
        Uses caching to prevent frequent API calls.
        """
        if self.test_mode:
            if secret_path not in self.test_secrets:
                return f"test_secret_{secret_path}"
            return self.test_secrets[secret_path]
            
        try:
            response = self.client.access_secret_version(request={"name": secret_path})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_path}: {str(e)}")
            raise
    
    def set_test_secret(self, secret_path: str, value: str) -> None:
        """Set a secret value for testing purposes."""
        if not self.test_mode:
            raise RuntimeError("set_test_secret can only be called in test mode")
        self.test_secrets[secret_path] = value 