from datetime import datetime, UTC
from typing import Dict, List, Any, Optional
import aiohttp
from loguru import logger
from api_pipeline.core.base import (
    BaseExtractor,
    ExtractorConfig,
    PaginationConfig,
    PaginationType,
    ParallelProcessingStrategy,
    TimeWindowParallelConfig,
    ProcessingPattern
)
from api_pipeline.core.utils import parse_datetime

class GitHubUsersExtractor(BaseExtractor):
    """Extractor for GitHub user details.
    
    This extractor fetches detailed information for GitHub users using their REST API.
    It uses the BATCH processing pattern as each user requires a separate API call
    to /users/{username} endpoint.
    
    Features:
    - Parallel processing of user batches
    - Rate limiting and retry handling
    - Detailed metrics tracking
    
    Required parameters:
    - usernames: List of GitHub usernames to fetch details for
    
    Example usage:
        config = ExtractorConfig(
            base_url="https://api.github.com",
            endpoints={"current": "/users/{username}"},
            auth_config=auth_config,
            processing_pattern=ProcessingPattern.BATCH,
            batch_parameter_name="usernames",
            batch_size=20,
            max_concurrent_requests=5
        )
        extractor = GitHubUsersExtractor(config)
        users = await extractor.extract({"usernames": ["torvalds", "gvanrossum"]})
    """
    
    def __init__(self, config: ExtractorConfig):
        # Ensure the endpoint template is set
        if "current" not in config.endpoints:
            config.endpoints["current"] = "/users/{username}"
        # Ensure we're using BATCH processing
        if config.processing_pattern != ProcessingPattern.BATCH:
            logger.warning("Forcing BATCH processing pattern for GitHubUsersExtractor")
            config.processing_pattern = ProcessingPattern.BATCH
        # Set batch parameter name if not set
        if not config.batch_parameter_name:
            config.batch_parameter_name = "usernames"
        super().__init__(config)

    def _get_item_params(self, item: str) -> Dict[str, str]:
        """Get parameters for a single user request.
        
        Args:
            item: GitHub username
            
        Returns:
            Dict with username parameter for URL template
        """
        return {"username": item}

    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single user from the API response.
        
        Args:
            item: Raw user data from GitHub API
            
        Returns:
            Transformed user data with selected fields
            
        Raises:
            KeyError: If required fields are missing
        """
        try:
            return {
                "username": item["login"],
                "id": item["id"],
                "name": item.get("name"),
                "company": item.get("company"),
                "blog": item.get("blog"),
                "location": item.get("location"),
                "email": item.get("email"),
                "bio": item.get("bio"),
                "twitter_username": item.get("twitter_username"),
                "public_repos": item["public_repos"],
                "public_gists": item["public_gists"],
                "followers": item["followers"],
                "following": item["following"],
                "created_at": parse_datetime(item["created_at"]),
                "updated_at": parse_datetime(item["updated_at"]),
                "url": item["html_url"],
                "type": item["type"],
                "repos_url": item["repos_url"]
            }
        except KeyError as e:
            logger.error(f"Failed to transform user data: {str(e)}")
            logger.error(f"Received item: {item}")
            raise

    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate extraction parameters.
        
        Args:
            parameters: Parameters to validate
            
        Raises:
            ValidationError: If parameters are invalid
        """
        if not parameters or "usernames" not in parameters:
            raise self.ValidationError("Parameter 'usernames' is required")
        
        usernames = parameters["usernames"]
        if not isinstance(usernames, list):
            raise self.ValidationError("Parameter 'usernames' must be a list")
        
        if not usernames:
            raise self.ValidationError("Parameter 'usernames' cannot be empty")
        
        if not all(isinstance(u, str) for u in usernames):
            raise self.ValidationError("All usernames must be strings") 