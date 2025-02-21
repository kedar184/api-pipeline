#!/usr/bin/env python3
import os
import asyncio
from datetime import datetime, UTC
from dotenv import load_dotenv
from loguru import logger
import pytest
import signal
from unittest.mock import AsyncMock, MagicMock
from typing import Dict, List, Any, Optional
import logging
from pathlib import Path

from api_pipeline.core.base import (
    BaseExtractor,
    ExtractorConfig,
    ProcessingPattern,
    PaginationConfig,
    AuthConfig,
    ParallelConfig
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

# Set a timeout for the entire test
TEST_TIMEOUT = 180  # Increase timeout to 180 seconds for API calls

@pytest.fixture
async def chain_executor():
    """Fixture to provide a chain executor with proper cleanup."""
    executor = ChainExecutor()
    yield executor
    # Cleanup will happen after the test

def timeout_handler(signum, frame):
    """Handle test timeout."""
    logger.error("Test execution timed out")
    raise TimeoutError("Test execution timed out")

@pytest.mark.asyncio
async def test_basic_chain(chain_executor):
    """Test basic chain functionality with repos and commits."""
    # Set up timeout
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(TEST_TIMEOUT)
    
    try:
        # Set up logging with more detail
        logger.remove()
        logger.add(lambda msg: print(msg, flush=True), level="DEBUG", 
                  format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
        logger.info("Starting basic chain test")
        
        # Create chain configuration using mock extractors
        chain_config = ChainConfig(
            chain_id="github_chain_test",
            extractors=[
                # Step 1: Get repositories
                ChainedExtractorConfig(
                    extractor_class="tests.integration.test_github_chain.MockReposExtractor",
                    extractor_config=ExtractorConfig(
                        base_url="https://api.github.com",
                        endpoints={"current": "/mock"},
                        processing_pattern=ProcessingPattern.SINGLE,
                        session_timeout=30
                    ).model_dump(),
                    input_mapping={"org": "organization"},
                    output_mapping={"full_name": "repository_list"},
                    processing_mode=ProcessingMode.SINGLE,
                    sequential_config=None,
                    parallel_config=None
                ),
                # Step 2: Get commits for each repository
                ChainedExtractorConfig(
                    extractor_class="tests.integration.test_github_chain.MockCommitsExtractor",
                    extractor_config=ExtractorConfig(
                        base_url="https://api.github.com",
                        endpoints={"current": "/mock"},
                        processing_pattern=ProcessingPattern.SINGLE,
                        session_timeout=30
                    ).model_dump(),
                    input_mapping={"repo": "repository_list"},
                    output_mapping={"sha": "commit_list"},
                    processing_mode=ProcessingMode.PARALLEL,
                    sequential_config=None,
                    parallel_config={
                        "max_concurrent": 10,
                        "error_handling": "continue",
                        "result_handling": "list"
                    }
                )
            ],
            max_parallel_chains=10,
            state_dir="./data/chain_state"
        )

        logger.info("Creating chain executor...")
        # Create executor
        executor = ChainExecutor()

        logger.info("Starting chain execution...")
        try:
            # Execute chain with test organization
            result = await executor.execute_single_chain(
                chain_config,
                {"organization": "pallets"}
            )
            
            logger.info(f"Chain execution completed. Result: {result}")
            
            # Verify results
            assert result is not None, "Result should not be None"
            
            # Verify repository list
            assert "repository_list" in result, "Should have repository_list in results"
            repository_list = result["repository_list"]
            logger.info(f"Repository list: {repository_list}")
            assert isinstance(repository_list, list), "repository_list should be a list"
            assert len(repository_list) == 5  # We expect 5 mock repos
            
            # Verify commit list
            assert "commit_list" in result, "Should have commit_list in results"
            commit_list = result["commit_list"]
            logger.info(f"Commit list: {commit_list}")
            assert isinstance(commit_list, list), "commit_list should be a list"
            assert len(commit_list) == 15  # 5 repos * 3 commits each
            
            # Log metrics
            metrics = executor.get_metrics()
            logger.info("Chain Executor Metrics:")
            logger.info(f"Chains executed: {metrics['chains_executed']}")
            logger.info(f"Total execution time: {metrics['total_execution_time']:.2f}s")
            logger.info(f"Items processed: {metrics['items_processed']}")
            logger.info(f"Errors encountered: {metrics['errors_encountered']}")
            
        except TimeoutError:
            logger.error("Test execution timed out")
            raise
        except Exception as e:
            logger.error(f"Error during chain execution: {str(e)}", exc_info=True)
            raise
    except TimeoutError:
        logger.error("Test execution timed out")
        raise
    except Exception as e:
        logger.error(f"Chain execution failed: {str(e)}")
        raise
    finally:
        logger.info("Test cleanup: Disabling alarm...")
        signal.alarm(0)

class MockReposExtractor(BaseExtractor):
    async def extract(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Simulate getting repos for an org
        assert parameters["org"] == "pallets"
        return [
            {"full_name": "pallets/flask"},
            {"full_name": "pallets/jinja"},
            {"full_name": "pallets/werkzeug"},
            {"full_name": "pallets/click"},
            {"full_name": "pallets/markupsafe"}
        ]

    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        pass

class MockCommitsExtractor(BaseExtractor):
    async def extract(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Simulate getting commits for a repo
        repo = parameters["repo"]
        return [
            {"sha": f"{repo}-commit-1"},
            {"sha": f"{repo}-commit-2"},
            {"sha": f"{repo}-commit-3"}
        ]

    def _validate(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        pass

@pytest.fixture
def chain_state_dir(tmp_path) -> Path:
    """Create a temporary directory for chain state."""
    return tmp_path / "chain_state"

@pytest.mark.asyncio
async def test_chain_executor_with_mocks(chain_state_dir):
    """Test chain executor with mocked extractors."""
    logger.info("Starting chain executor integration test with mocks")
    
    # Create chain configuration with mocked extractors
    chain_config = ChainConfig(
        chain_id="test_chain",
        extractors=[
            # Step 1: Get repositories
            ChainedExtractorConfig(
                extractor_class="tests.integration.test_github_chain.MockReposExtractor",
                extractor_config=ExtractorConfig(
                    base_url="https://api.example.com",
                    endpoints={"current": "/mock"},
                    auth_config={"auth_type": "bearer", "auth_credentials": {"token": ""}},
                    processing_pattern=ProcessingPattern.SINGLE
                ).model_dump(),
                input_mapping={
                    "org": "organization"
                },
                output_mapping={
                    "full_name": "repository_list"
                },
                processing_mode=ProcessingMode.SINGLE
            ),
            # Step 2: Get commits
            ChainedExtractorConfig(
                extractor_class="tests.integration.test_github_chain.MockCommitsExtractor",
                extractor_config=ExtractorConfig(
                    base_url="https://api.example.com",
                    endpoints={"current": "/mock"},
                    auth_config={"auth_type": "bearer", "auth_credentials": {"token": ""}},
                    processing_pattern=ProcessingPattern.SINGLE
                ).model_dump(),
                input_mapping={
                    "repo": "repository_list"
                },
                output_mapping={
                    "sha": "commit_list"
                },
                processing_mode=ProcessingMode.SEQUENTIAL,
                sequential_config=SequentialConfig(
                    error_handling=ErrorHandlingMode.CONTINUE,
                    result_handling=ResultMode.LIST
                )
            )
        ],
        max_parallel_chains=1,
        state_dir=str(chain_state_dir)
    )

    # Create chain executor
    executor = ChainExecutor()

    try:
        # Execute chain with test parameters
        parameters = {"organization": "test-org"}
        result = await executor.execute_single_chain(chain_config, parameters)

        # Verify results
        assert result is not None
        assert "repository_list" in result
        assert len(result["repository_list"]) == 2
        assert result["repository_list"][0] == "test-org/repo1"
        assert result["repository_list"][1] == "test-org/repo2"

        assert "commit_list" in result
        assert len(result["commit_list"]) == 4  # 2 commits per repo
        assert result["commit_list"][0] == "test-org/repo1-commit1"
        assert result["commit_list"][1] == "test-org/repo1-commit2"
        assert result["commit_list"][2] == "test-org/repo2-commit1"
        assert result["commit_list"][3] == "test-org/repo2-commit2"
    finally:
        # Clean up
        pass  # No cleanup needed

if __name__ == "__main__":
    asyncio.run(test_basic_chain(ChainExecutor()))