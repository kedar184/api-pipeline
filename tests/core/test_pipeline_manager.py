import pytest
from datetime import datetime, UTC, timedelta
from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock, patch
import os

from api_pipeline.core.pipeline_manager import PipelineManager
from api_pipeline.core.models import PipelineConfig, PipelineRun, PipelineRunStatus


@pytest.fixture
def manager():
    """Provide a pipeline manager instance."""
    return PipelineManager()


@pytest.fixture
def sample_pipeline_config() -> Dict:
    """Provide sample pipeline configuration."""
    return {
        "pipeline_id": "test-pipeline",
        "description": "Test Pipeline",
        "enabled": True,
        "extractor_class": "test.TestExtractor",
        "api_config": {
            "base_url": "https://api.test.com",
            "endpoints": {"test": "/test"}
        },
        "parameters": [],
        "output": []
    }


@pytest.fixture
def sample_run(sample_pipeline_config) -> PipelineRun:
    """Provide a sample pipeline run."""
    return PipelineRun(
        run_id="test-run-123",
        pipeline_id=sample_pipeline_config["pipeline_id"],
        status="running",
        start_time=datetime.now(UTC),
        parameters={}
    )


class TestPipelineManager:
    """Test suite for PipelineManager."""

    def test_get_pipeline_config(self, manager, sample_pipeline_config):
        """Test pipeline configuration retrieval."""
        with patch("api_pipeline.core.pipeline_manager.load_pipeline_config") as mock_load:
            mock_load.return_value = sample_pipeline_config
            
            config = manager._get_pipeline_config("test-pipeline")
            assert config.pipeline_id == "test-pipeline"
            assert config.enabled
            
            # Test caching
            config2 = manager._get_pipeline_config("test-pipeline")
            mock_load.assert_called_once()  # Should use cached value

    def test_get_pipeline_config_not_found(self, manager):
        """Test handling of non-existent pipeline configuration."""
        with patch("api_pipeline.core.pipeline_manager.load_pipeline_config") as mock_load:
            mock_load.side_effect = ValueError("Not found")
            
            with pytest.raises(ValueError, match="Pipeline .* not found"):
                manager._get_pipeline_config("non-existent")

    def test_get_extractor_class(self, manager):
        """Test extractor class loading."""
        # Test with valid module path
        from api_pipeline.core.base import BaseExtractor
        cls = manager._get_extractor_class("api_pipeline.core.base.BaseExtractor")
        assert cls == BaseExtractor
        
        # Test with invalid path
        with pytest.raises(ImportError):
            manager._get_extractor_class("invalid.module.Class")

    def test_list_pipelines(self, manager, sample_pipeline_config):
        """Test pipeline listing."""
        with patch("glob.glob") as mock_glob:
            mock_glob.return_value = ["pipeline1.yaml", "pipeline2.yaml"]
            
            with patch("builtins.open", create=True) as mock_open:
                mock_open.return_value.__enter__.return_value.read.return_value = """
                pipeline_id: test-pipeline
                description: Test Pipeline
                enabled: true
                """
                
                pipelines = manager.list_pipelines()
                assert len(pipelines) == 2
                assert all("pipeline_id" in p for p in pipelines)

    def test_get_pipeline_status(self, manager, sample_pipeline_config, sample_run):
        """Test pipeline status retrieval."""
        # Add a run to the manager
        manager.runs[sample_run.run_id] = sample_run
        
        with patch.object(manager, "_get_pipeline_config"):
            status = manager.get_pipeline_status("test-pipeline")
            assert status["pipeline_id"] == "test-pipeline"
            assert status["status"] == "running"
            assert "last_run" in status

    async def test_execute_pipeline(self, manager, sample_pipeline_config, sample_run):
        """Test pipeline execution."""
        # Mock pipeline factory and pipeline
        mock_pipeline = AsyncMock()
        mock_pipeline.execute = AsyncMock(return_value=sample_run)
        
        with patch("api_pipeline.core.pipeline_manager.PipelineFactory") as mock_factory:
            mock_factory.create_pipeline.return_value = mock_pipeline
            
            await manager.execute_pipeline(
                "test-pipeline",
                sample_run.run_id,
                {"param": "value"}
            )
            
            assert mock_pipeline.execute.called
            assert sample_run.status == "COMPLETED"

    async def test_execute_pipeline_error(self, manager, sample_pipeline_config, sample_run):
        """Test pipeline execution error handling."""
        mock_pipeline = AsyncMock()
        mock_pipeline.execute = AsyncMock(side_effect=Exception("Pipeline failed"))
        
        with patch("api_pipeline.core.pipeline_manager.PipelineFactory") as mock_factory:
            mock_factory.create_pipeline.return_value = mock_pipeline
            
            await manager.execute_pipeline(
                "test-pipeline",
                sample_run.run_id,
                {}
            )
            
            assert sample_run.status == "FAILED"
            assert "Pipeline failed" in sample_run.errors[0]

    async def test_list_pipeline_runs(self, manager, sample_run):
        """Test pipeline run listing with filters."""
        # Add some runs
        manager.runs[sample_run.run_id] = sample_run
        
        # Create another run with different status
        completed_run = PipelineRun(
            run_id="completed-run",
            pipeline_id="test-pipeline",
            status="completed",
            start_time=datetime.now(UTC) - timedelta(hours=1),
            end_time=datetime.now(UTC)
        )
        manager.runs[completed_run.run_id] = completed_run
        
        # Test filtering by status
        runs = await manager.list_pipeline_runs(
            "test-pipeline",
            status="running"
        )
        assert len(runs) == 1
        assert runs[0]["status"] == "running"
        
        # Test filtering by time range
        runs = await manager.list_pipeline_runs(
            "test-pipeline",
            start_time=datetime.now(UTC) - timedelta(minutes=30)
        )
        assert len(runs) == 1
        
        # Test limit
        runs = await manager.list_pipeline_runs(
            "test-pipeline",
            limit=1
        )
        assert len(runs) == 1

    async def test_trigger_pipeline(self, manager):
        """Test pipeline triggering."""
        result = await manager.trigger_pipeline(
            "test-pipeline",
            {"param": "value"}
        )
        
        assert "run_id" in result
        assert result["status"] == "triggered"
        assert result["run_id"] in manager.runs

    def test_get_run_status(self, manager, sample_run):
        """Test run status retrieval."""
        manager.runs[sample_run.run_id] = sample_run
        
        status = manager.get_run_status("test-pipeline", sample_run.run_id)
        assert status["run_id"] == sample_run.run_id
        assert status["pipeline_id"] == "test-pipeline"
        assert status["status"] == "running"
        
        # Test non-existent run
        with pytest.raises(KeyError):
            manager.get_run_status("test-pipeline", "non-existent-run") 