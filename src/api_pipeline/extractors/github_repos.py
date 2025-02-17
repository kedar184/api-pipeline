from datetime import datetime, UTC
from typing import Dict, List, Any, Optional
import aiohttp
from loguru import logger

from api_pipeline.core.base import (
    BaseExtractor,
    ExtractorConfig,
    PaginationConfig,
    PaginationType,
    ProcessingPattern
)
from api_pipeline.core.utils import parse_datetime

class GitHubReposExtractor(BaseExtractor):
    """Extractor for GitHub repository lists.
    
    This extractor fetches repository lists from GitHub using their REST API.
    It supports:
    - Link-based pagination for efficient retrieval
    - Filtering and sorting options
    - Rate limiting and retry handling
    
    The extractor uses a SINGLE processing pattern as GitHub's API returns
    multiple repositories in one request.
    """
    
    def __init__(self, config: ExtractorConfig):
        # Ensure we're using SINGLE processing pattern
        if config.processing_pattern != ProcessingPattern.SINGLE:
            logger.warning("Forcing SINGLE processing pattern for GitHubReposExtractor")
            config.processing_pattern = ProcessingPattern.SINGLE
        
        # Set up pagination if not configured
        if not config.pagination:
            config.pagination = PaginationConfig.with_link_header(
                page_size=100  # GitHub's max page size
            )
        
        super().__init__(config)
    
    async def _transform_item(self, item: dict) -> Dict[str, List[str]]:
        """Transform a single item from the GitHub API response.

        Args:
            item: A dictionary containing the GitHub API response for a single organization.

        Returns:
            A dictionary containing:
                - full_names: A list of repository full names
        """
        # The response is a list of repositories
        response_items = item if isinstance(item, list) else [item]
        full_names = []
        
        for repo in response_items:
            try:
                full_names.append(repo["full_name"])
            except KeyError as e:
                logger.error(f"Failed to transform repository: {str(e)}")
                logger.error(f"Received repo: {repo}")
                continue

        # Return a dictionary with the full_names list
        return {
            "full_names": full_names
        }
    
    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate extraction parameters.
        
        Args:
            parameters: Parameters to validate
            
        Raises:
            ValidationError: If parameters are invalid
        """
        if not parameters or "org" not in parameters:
            raise self.ValidationError("Parameter 'org' is required")