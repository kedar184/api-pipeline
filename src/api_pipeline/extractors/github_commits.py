from datetime import datetime, timezone, UTC, timedelta
from typing import Any, Dict, List, Optional
import aiohttp
from loguru import logger

from api_pipeline.core.base import BaseExtractor
from api_pipeline.core.auth import create_auth_handler


class GitHubCommitsExtractor(BaseExtractor):
    """Extracts repository commits with watermark-based incremental loading."""
    
    def __init__(self, config):
        super().__init__(config)
        self._watermark_store = {}  # In-memory store for demo, use proper storage in production
    
    def validate_parameters(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate extraction parameters."""
        parameters = parameters or {}
        
        # Validate repository format if provided
        repo = parameters.get("repo")
        if repo and not isinstance(repo, str):
            raise self.ValidationError("Repository must be a string in format 'owner/repo'")
        if repo and '/' not in repo:
            raise self.ValidationError("Repository must be in format 'owner/repo'")
    
    async def _ensure_session(self):
        """Initialize session with GitHub-specific headers."""
        if not self.session:
            # Get auth headers from auth handler
            self.auth_handler = create_auth_handler(self.config.auth_config)
            headers = await self.auth_handler.get_auth_headers()
            # Add GitHub-specific headers
            headers.update({
                "Accept": "application/vnd.github.v3+json"
            })
            self.session = aiohttp.ClientSession(headers=headers)

    async def _get_last_watermark(self) -> Optional[datetime]:
        """Get the last watermark value from storage."""
        repo = self.config.endpoints.get("repo", "default")
        stored_watermark = self._watermark_store.get(repo)
        
        if stored_watermark:
            return stored_watermark
        
        if self.config.watermark and self.config.watermark.initial_watermark:
            return self.config.watermark.initial_watermark
            
        # Default to 30 days lookback if no watermark
        return datetime.now(UTC) - timedelta(days=30)

    async def _update_watermark(self, new_watermark: datetime) -> None:
        """Update the watermark value in storage."""
        repo = self.config.endpoints.get("repo", "default")
        self._watermark_store[repo] = new_watermark
        self._metrics['watermark_updates'] += 1
        logger.info(f"Updated watermark for {repo} to {new_watermark.isoformat()}")

    def _apply_watermark_filters(
        self,
        params: Dict[str, Any],
        window_start: datetime,
        window_end: datetime
    ) -> Dict[str, Any]:
        """Apply GitHub-specific timestamp filters."""
        params = super()._apply_watermark_filters(params, window_start, window_end)
        
        # GitHub uses 'since' and 'until' for commit filtering
        params['since'] = window_start.isoformat()
        params['until'] = window_end.isoformat()
        
        return params

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
        """Extract commits with watermark-based windowing."""
        try:
            await self._ensure_session()
            parameters = parameters or {}
            
            # Get repository path
            repo_path = parameters.get("repo", "apache/spark")
            self.config.endpoints["repo"] = repo_path
            
            # Configure endpoint
            endpoint_override = f"/repos/{repo_path}/commits"
            
            # Use watermark-based extraction
            if self.config.watermark and self.config.watermark.enabled:
                self._current_watermark = await self._get_last_watermark()
                start_time = self._current_watermark
                
                # Calculate window bounds
                windows = self._get_window_bounds(start_time)
                logger.info(f"Processing {len(windows)} windows from {start_time}")
                
                all_data = []
                max_watermark = start_time
                
                # Process each window
                for window_start, window_end in windows:
                    self._metrics['window_count'] += 1
                    logger.info(f"Processing window: {window_start} to {window_end}")
                    
                    # Apply watermark filters
                    window_params = self._apply_watermark_filters(
                        parameters, window_start, window_end
                    )
                    
                    # Fetch commits for window
                    commits = await self._paginated_request(
                        "commits",
                        params=window_params,
                        endpoint_override=endpoint_override
                    )
                    
                    if not commits:
                        continue
                    
                    # Transform and process commits
                    for commit in commits:
                        transformed = await self._transform_item(commit)
                        all_data.append(transformed)
                        
                        # Update watermark based on commit timestamp
                        commit_time = transformed["committed_at"]
                        max_watermark = max(max_watermark, commit_time)
                
                # Update final watermark
                if max_watermark > start_time:
                    await self._update_watermark(max_watermark)
                
                return all_data
            
            else:
                # Non-watermark based extraction
                commits = await self._paginated_request(
                    "commits",
                    params=parameters,
                    endpoint_override=endpoint_override
                )
                
                return [await self._transform_item(commit) for commit in commits]
                
        finally:
            if self.session:
                await self.session.close()
                self.session = None 