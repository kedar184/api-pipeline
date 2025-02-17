#!/usr/bin/env python3
import os
import asyncio
from datetime import datetime, UTC
from dotenv import load_dotenv
from loguru import logger
import pytest

from api_pipeline.core.base import (
    ExtractorConfig,
    ProcessingPattern,
    PaginationConfig,
    AuthConfig
)
from api_pipeline.core.chain.models import (
    ChainConfig,
    ChainedExtractorConfig,
    ProcessingMode,
    ErrorHandlingMode,
    ResultMode,
    ParallelConfig
)
from api_pipeline.core.chain.executor import ChainExecutor

@pytest.mark.asyncio
async def test_basic_chain():
    """Test chain functionality with different processing modes."""
    # Set up logging
    logger.remove()  # Remove default handler
    logger.add(lambda msg: print(msg, flush=True), level="DEBUG")
    
    # Load environment variables
    dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
    load_dotenv(dotenv_path)
    
    github_token = os.getenv('LOCAL_GITHUB_TOKEN')
    if not github_token:
        raise ValueError("LOCAL_GITHUB_TOKEN environment variable is not set")
    
    # Create auth config
    auth_config = AuthConfig(
        auth_type="bearer",
        auth_credentials={"token": github_token}
    )
    
    # Base pagination config for GitHub API
    pagination_config = PaginationConfig.with_link_header(
        page_size=30,
        max_pages=1  # Limit to 1 page for testing
    )
    
    # Create chain configuration
    chain_config = ChainConfig(
        chain_id="github_chain_test",
        extractors=[
            # Step 1: Get repositories (SINGLE mode - one org returns list)
            ChainedExtractorConfig(
                extractor_class="api_pipeline.extractors.github_repos.GitHubReposExtractor",
                extractor_config=ExtractorConfig(
                    base_url="https://api.github.com",
                    endpoints={"current": "/orgs/{org}/repos"},
                    auth_config=auth_config.model_dump(),
                    processing_pattern=ProcessingPattern.SINGLE,
                    pagination=pagination_config.model_dump()
                ).model_dump(),
                input_mapping={
                    "org": "organization"
                },
                output_mapping={
                    "full_names": "repository_list"
                },
                processing_mode=ProcessingMode.SINGLE
            )
        ],
        max_parallel_chains=1,
        state_dir="./data/chain_state"
    )
    
    # Test with pallets organization (smaller than Apache)
    organization = "pallets"
    
    # Create executor and run chain
    executor = ChainExecutor()
    
    try:
        # Execute chain
        logger.info(f"Starting chain execution for {organization}")
        result = await executor.execute_single_chain(
            chain_config,
            {"organization": organization}
        )
        
        # Verify results
        assert result is not None
        assert "repository_list" in result
        assert isinstance(result["repository_list"], list)
        assert len(result["repository_list"]) > 0
        
        # Print results for debugging
        logger.info(f"Chain execution completed successfully")
        logger.info(f"Found repositories: {result['repository_list']}")
        
    except Exception as e:
        logger.error(f"Chain execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(test_basic_chain())