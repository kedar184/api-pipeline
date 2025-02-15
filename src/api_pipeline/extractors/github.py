from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import aiohttp
from loguru import logger

from api_pipeline.core.base import BaseExtractor
from api_pipeline.core.auth import create_auth_handler


class GitHubExtractor(BaseExtractor):
    """Fetches repository information and then chains to fetch issues for each repository."""
    
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

    async def extract(self, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Extract data from GitHub API with chaining."""
        try:
            await self._ensure_session()
            parameters = parameters or {}
            org_name = parameters.get("org", "apache")
            
            # Get repositories for organization
            repos = await self._paginated_request(
                "repos",
                params={"org": org_name}
            )
            
            # Chain to get issues for each repository
            all_data = []
            for repo in repos:
                repo_name = repo["name"]
                logger.info(f"Fetching issues for repository: {repo_name}")
                
                try:
                    issues = await self._paginated_request(
                        "issues",
                        params={"state": "all"},
                        endpoint_override=f"/repos/{org_name}/{repo_name}/issues"
                    )
                    
                    all_data.extend([{
                        "repo_id": repo["id"],
                        "repo_name": repo_name,
                        "repo_stars": repo["stargazers_count"],
                        "repo_forks": repo["forks_count"],
                        "issue_id": issue["id"],
                        "issue_number": issue["number"],
                        "issue_title": issue["title"],
                        "issue_state": issue["state"],
                        "issue_created_at": issue["created_at"],
                        "issue_updated_at": issue["updated_at"],
                        "issue_closed_at": issue["closed_at"],
                    } for issue in issues])
                    
                except Exception as e:
                    logger.error(f"Error fetching issues for {repo_name}: {str(e)}")
                    continue
            
            return all_data
        finally:
            if self.session:
                await self.session.close()
                self.session = None

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform the extracted data for BigQuery.
        
        Converts GitHub's ISO-8601 timestamp strings to Python datetime objects
        with UTC timezone for BigQuery compatibility."""
        for item in data:
            for ts_field in ["issue_created_at", "issue_updated_at", "issue_closed_at"]:
                if item[ts_field]:
                    item[ts_field] = datetime.strptime(
                        item[ts_field], 
                        "%Y-%m-%dT%H:%M:%SZ"
                    ).replace(tzinfo=timezone.utc)
        
        return data