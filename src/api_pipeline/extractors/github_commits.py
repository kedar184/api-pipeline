from datetime import datetime, timezone, UTC, timedelta
from typing import Any, Dict, List, Optional, Tuple
import aiohttp
from loguru import logger
import asyncio

from api_pipeline.core.base import BaseExtractor
from api_pipeline.core.auth import create_auth_handler


class GitHubCommitsExtractor(BaseExtractor):
    """Extracts repository commits with watermark-based incremental loading."""
    
    def __init__(self, config):
        super().__init__(config)
        self._watermark_store = {}  # In-memory store for demo, use proper storage in production
    
    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate GitHub-specific parameters."""
        parameters = parameters or {}
        
        # Validate repository format if provided
        repo = parameters.get("repo")
        if repo and not isinstance(repo, str):
            raise self.ValidationError("Repository must be a string in format 'owner/repo'")
        if repo and '/' not in repo:
            raise self.ValidationError("Repository must be in format 'owner/repo'")
    
    def _get_additional_headers(self) -> Dict[str, str]:
        """Add GitHub-specific headers."""
        return {
            "Accept": "application/vnd.github.v3+json"
        }

    def _get_watermark_key(self) -> str:
        """Use repository name as watermark key."""
        return self.config.endpoints.get("repo", "default")

    def _get_endpoint_override(self, parameters: Dict[str, Any]) -> Optional[str]:
        """Get GitHub-specific endpoint with repository path."""
        repo = parameters.get("repo")  # Get repo without removing it
        if repo:
            return f"/repos/{repo}/commits"
        return None

    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a commit into standardized format."""
        return {
            "commit_id": item["sha"],
            "repo_name": self.config.endpoints["repo"],
            "author_name": item["commit"]["author"]["name"],
            "author_email": item["commit"]["author"]["email"],
            "message": item["commit"]["message"],
            "commit_url": item["html_url"],
            "committed_at": datetime.strptime(
                item["commit"]["author"]["date"],
                "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=UTC),
            "files_changed": len(item.get("files", [])) if "files" in item else None,
            "additions": item.get("stats", {}).get("additions"),
            "deletions": item.get("stats", {}).get("deletions"),
            "processed_at": datetime.now(UTC)
        }

    async def extract(self, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Extract commits with parallel window processing."""
        try:
            await self._ensure_session()
            parameters = parameters or {}
            
            # Get repository path
            repo_path = parameters.get("repo", "apache/spark")
            self.config.endpoints["repo"] = repo_path
            
            # Use watermark-based extraction
            if self.config.watermark and self.config.watermark.enabled:
                self._current_watermark = await self._get_last_watermark()
                start_time = self._current_watermark
                
                # Calculate window bounds
                windows = self._get_window_bounds(start_time)
                logger.info(f"Processing {len(windows)} windows from {start_time}")
                
                # Process windows in parallel with concurrency limit
                semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
                async def process_with_semaphore(window):
                    async with semaphore:
                        return await self._process_window(window[0], window[1], parameters)
                
                window_results = await asyncio.gather(
                    *[process_with_semaphore(window) for window in windows]
                )
                
                # Flatten results and update watermark
                all_data = [item for sublist in window_results for item in sublist]
                
                if all_data:
                    max_watermark = max(
                        commit["committed_at"] for commit in all_data
                    )
                    if max_watermark > start_time:
                        await self._update_watermark(max_watermark)
                
                return all_data
            
            else:
                # Non-watermark based extraction
                commits = await self._paginated_request(
                    "commits",
                    params=parameters,
                    endpoint_override=f"/repos/{repo_path}/commits"
                )
                
                return [await self._transform_item(commit) for commit in commits]
                
        finally:
            if self.session:
                await self.session.close()
                self.session = None 