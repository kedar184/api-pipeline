from datetime import datetime, UTC
from typing import Dict, Any, Optional
from loguru import logger
from api_pipeline.core.base import (
    BaseExtractor,
    ExtractorConfig
)

class GitHubCommitsExtractor(BaseExtractor):
    def __init__(self, config: ExtractorConfig):
        super().__init__(config)

    def _get_url_params(self, parameters: Dict[str, Any]) -> Dict[str, str]:
        return {"repo": parameters["repo"]}

    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return {
                "sha": item["sha"],
                "commit": item["commit"],
                "url": item["url"],
                "html_url": item["html_url"],
                "author": item["author"],
                "committer": item["committer"],
                "parents": item["parents"],
                # Add timestamp for watermark tracking
                "committed_at": datetime.fromisoformat(item["commit"]["author"]["date"]).strftime("%Y-%m-%dT%H:%M:%SZ")
            }
        except KeyError as e:
            logger.error(f"Failed to transform commit: {str(e)}")
            logger.error(f"Received commit: {item}")
            return {}

    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        if not parameters or not isinstance(parameters.get("repo"), str):
            raise self.ValidationError("'repo' parameter must be a string")