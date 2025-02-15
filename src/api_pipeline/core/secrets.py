from typing import Optional
from functools import lru_cache
import os

from google.cloud import secretmanager
from loguru import logger


class SecretManager:
    """Handles secret retrieval from Google Cloud Secret Manager."""
    
    def __init__(self, project_id: Optional[str] = None):
        self.project_id = project_id or os.getenv('GOOGLE_CLOUD_PROJECT')
        if not self.project_id:
            raise ValueError("Project ID must be provided or set in GOOGLE_CLOUD_PROJECT environment variable")
        
        self.client = secretmanager.SecretManagerServiceClient()
    
    @lru_cache(maxsize=100)
    def get_secret(self, secret_path: str) -> str:
        """
        Get a secret value from Secret Manager using its full path.
        Uses caching to prevent frequent API calls.
        """
        try:
            response = self.client.access_secret_version(request={"name": secret_path})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_path}: {str(e)}")
            raise 