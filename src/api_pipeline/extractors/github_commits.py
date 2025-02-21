from datetime import datetime, UTC
from typing import Dict, Any, Optional, List
from loguru import logger
from api_pipeline.core.base import (
    BaseExtractor,
    ExtractorConfig
)

class GitHubCommitsExtractor(BaseExtractor):
    """Extractor for GitHub commits.
    
    Usage:
        config = ExtractorConfig(
            base_url="https://api.github.com",
            endpoints={"current": "/repos/{repo}/commits"},  # Framework handles parameter substitution
            auth_config=auth_config,
            ...
        )
    """

    async def _ensure_session(self):
        """Initialize session with GitHub-specific headers."""
        await super()._ensure_session()
        if self.session:
            # Add GitHub-specific headers
            self.session.headers.update({
                "Accept": "application/vnd.github.v3+json"
            })

    def _get_url_params(self, parameters: Dict[str, Any]) -> Dict[str, str]:
        """Get URL parameters for the request."""
        params = {"repo": parameters["repo"]}
        if "per_page" in parameters:
            params["per_page"] = str(parameters["per_page"])
        if "page" in parameters:
            params["page"] = str(parameters["page"])
        return params

    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate the parameters for the commits extractor."""
        if not parameters or "repo" not in parameters:
            raise ValueError("'repo' parameter is required")
        if not isinstance(parameters["repo"], str):
            raise ValueError("'repo' parameter must be a string")
            
        logger.debug("Processing repository: {}", parameters["repo"])

    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a commit item."""
        logger.debug("Transforming commit: {}", item.get('sha', 'unknown'))
        return {
            "sha": item.get("sha"),
            "commit": item.get("commit", {}),
            "url": item.get("url"),
            "html_url": item.get("html_url")
        }