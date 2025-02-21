#!/usr/bin/env python3
import os
import asyncio
from dotenv import load_dotenv
from loguru import logger
import pytest

from api_pipeline.core.base import (
    ExtractorConfig,
    ProcessingPattern,
    PaginationConfig,
    AuthConfig
)
from api_pipeline.extractors.github_repos import GitHubReposExtractor

@pytest.mark.asyncio
async def test_github_repos_extractor():
    """Test GitHubReposExtractor in isolation."""
    # Set up logging
    logger.remove()
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
    
    # Create pagination config - limit to 2 repos for testing
    pagination_config = PaginationConfig.with_link_header(
        page_size=2,
        max_pages=1
    )
    
    # Create extractor config
    config = ExtractorConfig(
        base_url="https://api.github.com",
        endpoints={"current": "/orgs/{org}/repos"},
        auth_config=auth_config.model_dump(),
        processing_pattern=ProcessingPattern.SINGLE,
        pagination=pagination_config.model_dump(),
        session_timeout=10
    )
    
    # Create and test extractor
    extractor = GitHubReposExtractor(config)
    
    try:
        # Test with pallets organization
        logger.info("Starting extraction test")
        result = await extractor.extract({"org": "pallets"})
        
        # Log the raw result for inspection
        logger.info("Raw extraction result:")
        logger.info(result)
        
        # Basic assertions
        assert result is not None, "Result should not be None"
        assert isinstance(result, list), "Result should be a list"
        assert len(result) > 0, "Should have at least one result"
        
        # Log the structure of the first result
        if result:
            logger.info("First result structure:")
            logger.info(f"Type: {type(result[0])}")
            logger.info(f"Keys: {result[0].keys() if isinstance(result[0], dict) else 'Not a dict'}")
            logger.info(f"Content: {result[0]}")
            
            # If it's a dict with transformed_items
            if isinstance(result[0], dict) and 'transformed_items' in result[0]:
                logger.info("Transformed items in first result:")
                logger.info(result[0]['transformed_items'])
        
    finally:
        # Cleanup
        await extractor.cleanup()

if __name__ == "__main__":
    asyncio.run(test_github_repos_extractor()) 