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
            logger.info("No pagination config provided, using default Link header pagination")
            config.pagination = PaginationConfig.with_link_header(
                page_size=100  # GitHub's max page size
            )
        else:
            logger.info(f"Using provided pagination config: page_size={config.pagination.page_size}, max_pages={config.pagination.max_pages}")
        
        super().__init__(config)
    
    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, str]:
        """Transform a single repository item into a dictionary with full_name.

        Args:
            item: A dictionary containing repository information from the GitHub API.

        Returns:
            A dictionary containing the full_name of the repository.
        """
        # The response is a list of repositories
        if isinstance(item, list):
            # Take the first item since we're transforming one at a time
            item = item[0]
            
        logger.debug(f"Transforming repository: {item.get('full_name', 'unknown')}")
        return {"full_name": item["full_name"]}
    
    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate extraction parameters.
        
        Args:
            parameters: Parameters to validate
            
        Raises:
            ValidationError: If parameters are invalid
        """
        if not parameters or "org" not in parameters:
            raise self.ValidationError("Parameter 'org' is required")
        
        logger.info(f"Validated parameters for organization: {parameters['org']}")
    
    async def extract(self, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Extract repository data from GitHub API.
        
        Args:
            parameters: Extraction parameters including 'org'
            
        Returns:
            List of repository data
        """
        logger.info(f"Starting repository extraction for parameters: {parameters}")
        
        try:
            results = await super().extract(parameters)
            logger.info(f"Successfully extracted {len(results)} repositories")
            logger.debug(f"Repository names: {[r.get('full_name', 'unknown') for r in results]}")
            return results
            
        except Exception as e:
            logger.error(f"Error during repository extraction: {str(e)}")
            raise
    
    async def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None, endpoint_override: Optional[str] = None) -> Dict[str, Any]:
        """Make a request to the GitHub API with detailed logging.
        
        Args:
            endpoint: API endpoint to call
            params: Request parameters
            endpoint_override: Optional endpoint override
            
        Returns:
            API response data
        """
        url = self._get_url(endpoint, params or {}, endpoint_override)
        logger.info(f"Making GitHub API request to: {url}")
        
        try:
            response = await super()._make_request(endpoint, params, endpoint_override)
            
            # Log pagination information from headers
            if hasattr(response, 'headers'):
                link_header = response.headers.get('Link', '')
                logger.info(f"Pagination Link header: {link_header}")
                
                remaining = response.headers.get('X-RateLimit-Remaining', 'unknown')
                logger.info(f"Rate limit remaining: {remaining}")
            
            logger.debug(f"Response data: {response}")
            return response
            
        except Exception as e:
            logger.error(f"Error making GitHub API request: {str(e)}")
            raise