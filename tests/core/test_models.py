import pytest
from datetime import datetime, UTC, timedelta
from typing import Dict, List
from pydantic import ValidationError

from api_pipeline.core.models import (
    PipelineParameter,
    PipelineConfig,
    PipelineRunStatus,
    PipelineRun,
    PipelineRunRequest,
    PipelineStatus
)


class TestPipelineParameter:
    """Test suite for PipelineParameter model."""

    def test_valid_parameter(self):
        """Test valid parameter creation."""
        param = PipelineParameter(
            name="test_param",
            type="string",
            required=True,
            description="Test parameter",
            default="test"
        )
        assert param.name == "test_param"
        assert param.type == "string"
        assert param.required
        assert param.description == "Test parameter"
        assert param.default == "test"

    def test_type_validation(self):
        """Test parameter type validation."""
        # Test valid types
        valid_types = ["string", "integer", "float", "boolean", "array", "object"]
        for type_name in valid_types:
            param = PipelineParameter(name="test", type=type_name)
            assert param.type == type_name.lower()

        # Test invalid type
        with pytest.raises(ValidationError):
            PipelineParameter(name="test", type="invalid_type")

    def test_optional_fields(self):
        """Test optional parameter fields."""
        param = PipelineParameter(name="test", type="string")
        assert not param.required
        assert param.description is None
        assert param.default is None


class TestPipelineConfig:
    """Test suite for PipelineConfig model."""

    @pytest.fixture
    def valid_config_data(self) -> Dict:
        """Provide valid configuration data."""
        return {
            "pipeline_id": "test-pipeline",
            "description": "Test pipeline",
            "enabled": True,
            "extractor_class": "test.extractor.TestExtractor",
            "api_config": {
                "base_url": "https://api.test.com",
                "endpoints": {"test": "/test"}
            },
            "parameters": [
                {
                    "name": "param1",
                    "type": "string",
                    "required": True
                }
            ],
            "output": [
                {
                    "type": "test_output",
                    "config": {"test": "value"}
                }
            ]
        }

    def test_valid_config(self, valid_config_data):
        """Test valid configuration creation."""
        config = PipelineConfig(**valid_config_data)
        assert config.pipeline_id == "test-pipeline"
        assert config.enabled
        assert len(config.parameters) == 1
        assert len(config.output) == 1

    def test_pipeline_id_validation(self):
        """Test pipeline ID validation."""
        # Valid IDs
        valid_ids = ["test-123", "test_123", "test123"]
        for pid in valid_ids:
            PipelineConfig(
                pipeline_id=pid,
                description="test",
                extractor_class="test.Extractor",
                api_config={},
                output=[]
            )

        # Invalid ID
        with pytest.raises(ValidationError):
            PipelineConfig(
                pipeline_id="test@123",
                description="test",
                extractor_class="test.Extractor",
                api_config={},
                output=[]
            )

    def test_extractor_class_validation(self):
        """Test extractor class path validation."""
        # Valid paths
        valid_paths = [
            "test.Extractor",
            "module.submodule.Extractor",
            "my_module.MyExtractor"
        ]
        for path in valid_paths:
            PipelineConfig(
                pipeline_id="test",
                description="test",
                extractor_class=path,
                api_config={},
                output=[]
            )

        # Invalid path
        with pytest.raises(ValidationError):
            PipelineConfig(
                pipeline_id="test",
                description="test",
                extractor_class="invalid.path.",
                api_config={},
                output=[]
            )


class TestPipelineRunStatus:
    """Test suite for PipelineRunStatus model."""

    def test_valid_status(self):
        """Test valid status creation."""
        status = PipelineRunStatus(
            status="running",
            message="Processing data"
        )
        assert status.status == "running"
        assert status.message == "Processing data"
        assert isinstance(status.timestamp, datetime)

    def test_status_validation(self):
        """Test status value validation."""
        valid_statuses = ["pending", "running", "completed", "failed", "cancelled"]
        for status_value in valid_statuses:
            status = PipelineRunStatus(status=status_value)
            assert status.status == status_value

        with pytest.raises(ValidationError):
            PipelineRunStatus(status="invalid_status")

    def test_timestamp_auto_generation(self):
        """Test automatic timestamp generation."""
        before = datetime.now(UTC)
        status = PipelineRunStatus(status="running")
        after = datetime.now(UTC)
        
        assert before <= status.timestamp <= after


class TestPipelineRun:
    """Test suite for PipelineRun model."""

    @pytest.fixture
    def valid_run_data(self) -> Dict:
        """Provide valid run data."""
        return {
            "run_id": "test-run-123",
            "pipeline_id": "test-pipeline",
            "status": "running",
            "start_time": datetime.now(UTC),
            "parameters": {"param": "value"}
        }

    def test_valid_run(self, valid_run_data):
        """Test valid run creation."""
        run = PipelineRun(**valid_run_data)
        assert run.run_id == "test-run-123"
        assert run.status == "running"
        assert run.records_processed == 0
        assert len(run.errors) == 0

    def test_end_time_validation(self, valid_run_data):
        """Test end time validation."""
        # Valid end time
        valid_run_data["end_time"] = valid_run_data["start_time"] + timedelta(minutes=5)
        run = PipelineRun(**valid_run_data)
        assert run.end_time > run.start_time

        # Invalid end time (before start time)
        invalid_data = valid_run_data.copy()
        invalid_data["end_time"] = valid_run_data["start_time"] - timedelta(minutes=5)
        with pytest.raises(ValidationError):
            PipelineRun(**invalid_data)

    def test_status_updates(self, valid_run_data):
        """Test run status updates."""
        run = PipelineRun(**valid_run_data)
        
        # Add status updates
        run.statuses.append(PipelineRunStatus(
            status="completed",
            message="Processing finished"
        ))
        
        assert len(run.statuses) == 1
        assert run.statuses[0].status == "completed"


class TestPipelineRunRequest:
    """Test suite for PipelineRunRequest model."""

    def test_empty_request(self):
        """Test request with no parameters."""
        request = PipelineRunRequest()
        assert request.parameters is None
        assert request.output_config is None

    def test_full_request(self):
        """Test request with all fields."""
        request = PipelineRunRequest(
            parameters={"param": "value"},
            output_config={"format": "json"}
        )
        assert request.parameters == {"param": "value"}
        assert request.output_config == {"format": "json"}


class TestPipelineStatus:
    """Test suite for PipelineStatus model."""

    def test_valid_status(self):
        """Test valid pipeline status creation."""
        status = PipelineStatus(
            pipeline_id="test-pipeline",
            status="running",
            last_run="2024-02-12T12:00:00Z",
            records_processed=100,
            errors=["Error 1", "Error 2"]
        )
        assert status.pipeline_id == "test-pipeline"
        assert status.status == "running"
        assert status.records_processed == 100
        assert len(status.errors) == 2

    def test_minimal_status(self):
        """Test minimal pipeline status creation."""
        status = PipelineStatus(
            pipeline_id="test-pipeline",
            status="pending",
            last_run="2024-02-12T12:00:00Z"
        )
        assert status.records_processed == 0
        assert len(status.errors) == 0 