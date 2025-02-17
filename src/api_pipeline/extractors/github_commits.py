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

class GitHubCommitsExtractor(BaseExtractor):
    """Extractor for GitHub repository commits.
    
    This extractor fetches commits from a GitHub repository using their REST API.
    It supports:
    - Time-based filtering using 'since' and 'until' parameters
    - Link-based pagination for efficient retrieval of large commit histories
    - Watermarking for incremental updates
    
    The extractor uses a SINGLE processing pattern as GitHub's API returns
    multiple commits in one request, rather than requiring separate requests
    for each commit.
    
    Required URL parameters:
    - repo: The repository in format "owner/repo" (e.g., "apache/spark")
    """
    
    def __init__(self, config: ExtractorConfig):
        # Ensure the endpoint template is set
        if "current" not in config.endpoints:
            config.endpoints["current"] = "/repos/{repo}/commits"
        super().__init__(config)

    def _get_url_params(self, parameters: Dict[str, Any]) -> Dict[str, str]:
        """Get URL template parameters from the request parameters.
        
        This method extracts values needed for the URL template (e.g. {repo})
        from the request parameters. These values are used to construct the
        final URL path.
        Example:
            If endpoint template is "/repos/{repo}/commits" and parameters
            contains {"repo": "apache/spark"}, this returns {"repo": "apache/spark"}
        """
        return {
            "repo": parameters["repo"]  # Required by _validate
        }

    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single commit from the API response.
        
        """
        try:
            return {
                "commit_id": item["sha"],
                "committed_at": parse_datetime(item["commit"]["author"]["date"]),
                "author_name": item["commit"]["author"]["name"],
                "author_email": item["commit"]["author"]["email"],
                "message": item["commit"]["message"],
                "url": item["html_url"]
            }
        except KeyError as e:
            logger.error(f"Failed to transform commit: {str(e)}")
            logger.error(f"Received item: {item}")
            raise

    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate extraction parameters.
        """
        if not parameters or "repo" not in parameters:
            raise self.ValidationError("Repository parameter 'repo' is required")