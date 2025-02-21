#!/usr/bin/env python3
from typing import Dict, List, Any, Optional
import pytest
from loguru import logger

from api_pipeline.core.base import (
    BaseExtractor,
    ExtractorConfig,
    ProcessingPattern,
    AuthConfig
)
from api_pipeline.core.chain.models import (
    ChainConfig,
    ChainedExtractorConfig,
    ProcessingMode,
    ErrorHandlingMode,
    ResultMode,
    SequentialConfig
)
from api_pipeline.core.chain.executor import ChainExecutor

class MockReposExtractor(BaseExtractor):
    """Mock extractor that returns a fixed list of repositories."""
    async def extract(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return mock repository data."""
        org = parameters["org"]
        return [
            {"full_name": f"{org}/repo1", "id": 1},
            {"full_name": f"{org}/repo2", "id": 2}
        ]

    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate parameters."""
        if not parameters or "org" not in parameters:
            raise ValueError("org parameter is required")

    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single item."""
        return {
            "full_name": item["full_name"]  # Only return the full_name field
        }

class MockCommitsExtractor(BaseExtractor):
    """Mock extractor that returns a fixed list of commits for a repository."""
    async def extract(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return mock commit data for a repository."""
        repo = parameters["repo"]
        # Handle both string and dict inputs for repo
        repo_name = repo["full_name"] if isinstance(repo, dict) else repo
        return [
            {
                "sha": f"{repo_name}-commit1",
                "commit": {"message": f"Test commit 1 for {repo_name}"},
                "repository": repo_name
            },
            {
                "sha": f"{repo_name}-commit2", 
                "commit": {"message": f"Test commit 2 for {repo_name}"},
                "repository": repo_name
            }
        ]

    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate parameters."""
        if not parameters or "repo" not in parameters:
            raise ValueError("repo parameter is required")

    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single item."""
        return item

class ErrorReposExtractor(BaseExtractor):
    """Mock extractor that simulates an error during extraction."""
    async def extract(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Raise an error during extraction."""
        raise ValueError("Simulated error in repository extraction")

    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Validate parameters."""
        pass

    async def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single item."""
        return item

@pytest.mark.asyncio
async def test_chain_executor_sequential_processing():
    """Test chain executor with sequential processing of mock data."""
    logger.info("Starting chain executor unit test with sequential processing")

    # Create chain configuration with mocked extractors
    chain_config = ChainConfig(
        chain_id="test_chain",
        extractors=[
            # Step 1: Get repositories (SINGLE mode)
            ChainedExtractorConfig(
                extractor_class="tests.chain.test_chain_executor.MockReposExtractor",
                extractor_config=ExtractorConfig(
                    base_url="https://api.example.com",
                    endpoints={"current": "/mock"},
                    auth_config=AuthConfig(
                        auth_type="bearer",
                        auth_credentials={"token": ""}
                    ).model_dump(),
                    processing_pattern=ProcessingPattern.SINGLE
                ).model_dump(),
                input_mapping={
                    "org": "organization"
                },
                output_mapping={
                    "full_name": "repository_list"  # Map the full_name field to repository_list
                },
                processing_mode=ProcessingMode.SINGLE
            ),
            # Step 2: Get commits (SEQUENTIAL mode)
            ChainedExtractorConfig(
                extractor_class="tests.chain.test_chain_executor.MockCommitsExtractor",
                extractor_config=ExtractorConfig(
                    base_url="https://api.example.com",
                    endpoints={"current": "/mock"},
                    auth_config=AuthConfig(
                        auth_type="bearer",
                        auth_credentials={"token": ""}
                    ).model_dump(),
                    processing_pattern=ProcessingPattern.SINGLE
                ).model_dump(),
                input_mapping={
                    "repo": "repository_list"  # Map from the list being processed
                },
                output_mapping={
                    "sha": "commit_list",  # Map the SHA field to commit_list
                    "commit": "commit_details",  # Map commit details
                    "repository": "commit_repos"  # Map repository info
                },
                processing_mode=ProcessingMode.SEQUENTIAL,
                sequential_config=SequentialConfig(
                    error_handling=ErrorHandlingMode.CONTINUE,
                    result_handling=ResultMode.LIST
                )
            )
        ],
        max_parallel_chains=1,
        state_dir="./data/chain_state"
    )

    # Create executor
    executor = ChainExecutor()

    # Execute chain with test organization
    logger.info("Executing chain with test organization")
    result = await executor.execute_single_chain(
        chain_config,
        {"organization": "test-org"}
    )

    # Verify results
    logger.info(f"Chain execution result: {result}")

    # Verify repositories
    assert "repository_list" in result, "Should have repository_list in results"
    repos = result["repository_list"]
    assert isinstance(repos, list), "repository_list should be a list"
    assert len(repos) == 2, "Should have 2 repositories"
    assert repos[0] == "test-org/repo1", "First repo should match"
    assert repos[1] == "test-org/repo2", "Second repo should match"

    # Verify commits
    assert "commit_list" in result, "Should have commit_list in results"
    commits = result["commit_list"]
    assert isinstance(commits, list), "commit_list should be a list"
    assert len(commits) == 4, "Should have 4 commits (2 per repository)"
    
    # Verify commit data
    expected_shas = [
        "test-org/repo1-commit1",
        "test-org/repo1-commit2",
        "test-org/repo2-commit1",
        "test-org/repo2-commit2"
    ]
    assert commits == expected_shas, "Commit SHAs should match expected order"

    # Verify commit details are present
    assert "commit_details" in result, "Should have commit details"
    commit_details = result["commit_details"]
    assert isinstance(commit_details, list), "commit_details should be a list"
    assert len(commit_details) == 4, "Should have 4 sets of commit details"
    
    # Verify repository mapping
    assert "commit_repos" in result, "Should have commit repositories"
    commit_repos = result["commit_repos"]
    assert isinstance(commit_repos, list), "commit_repos should be a list"
    assert len(commit_repos) == 4, "Should have 4 repository entries"
    assert all(repo in ["test-org/repo1", "test-org/repo2"] for repo in commit_repos), "Repository names should match"

    logger.info("Chain executor unit test completed successfully")

@pytest.mark.asyncio
async def test_chain_executor_error_handling():
    """Test chain executor error handling with mock data."""
    logger.info("Starting chain executor error handling test")
    
    # Create chain config with error-generating extractor
    chain_config = ChainConfig(
        chain_id="error_test_chain",
        extractors=[
            ChainedExtractorConfig(
                extractor_class="tests.chain.test_chain_executor.ErrorReposExtractor",
                extractor_config=ExtractorConfig(
                    base_url="https://api.example.com",
                    endpoints={"current": "/mock"},
                    auth_config=AuthConfig(
                        auth_type="bearer",
                        auth_credentials={"token": ""}
                    ).model_dump(),
                    processing_pattern=ProcessingPattern.SINGLE
                ).model_dump(),
                input_mapping={
                    "org": "organization"
                },
                output_mapping={
                    "full_name": "repository_list"
                },
                processing_mode=ProcessingMode.SINGLE
            )
        ],
        max_parallel_chains=1,
        state_dir="./data/chain_state"
    )
    
    executor = ChainExecutor()
    
    # Verify error is propagated
    with pytest.raises(ValueError, match="Simulated error in repository extraction"):
        await executor.execute_single_chain(
            chain_config,
            {"organization": "test-org"}
        )
    
    logger.info("Chain executor error handling test completed successfully") 