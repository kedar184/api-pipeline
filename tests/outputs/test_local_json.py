import json
import pytest
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List

from api_pipeline.core.base import OutputConfig
from api_pipeline.outputs.local_json import LocalJsonOutput


@pytest.fixture
def temp_dir(tmp_path):
    """Provide a temporary directory for test outputs."""
    return tmp_path


@pytest.fixture
def sample_data() -> List[Dict]:
    """Provide sample test data."""
    return [
        {
            "timestamp": "2024-02-12T12:00:00Z",
            "location_id": "5128581",
            "temperature": 15.6,
            "org": "test-org"
        },
        {
            "timestamp": "2024-02-12T12:00:00Z",
            "location_id": "2643743",
            "temperature": 12.3,
            "org": "test-org"
        }
    ]


@pytest.fixture
def basic_config(temp_dir) -> OutputConfig:
    """Provide basic output configuration."""
    return OutputConfig(
        type="local_json",
        enabled=True,
        config={
            "output_dir": str(temp_dir),
            "file_format": "jsonl"
        }
    )


@pytest.fixture
def partitioned_config(temp_dir) -> OutputConfig:
    """Provide configuration with partitioning."""
    return OutputConfig(
        type="local_json",
        enabled=True,
        config={
            "output_dir": str(temp_dir),
            "file_format": "jsonl",
            "partition_by": "org"
        }
    )


@pytest.fixture
def multi_partition_config(temp_dir) -> OutputConfig:
    """Provide configuration with multiple partition fields."""
    return OutputConfig(
        type="local_json",
        enabled=True,
        config={
            "output_dir": str(temp_dir),
            "file_format": "jsonl",
            "partition_by": ["org", "location_id"]
        }
    )


@pytest.fixture
def date_partition_config(temp_dir) -> OutputConfig:
    """Provide configuration with date-based partitioning."""
    return OutputConfig(
        type="local_json",
        enabled=True,
        config={
            "output_dir": str(temp_dir),
            "file_format": "jsonl",
            "partition_by": "date"
        }
    )


class TestLocalJsonOutput:
    """Test suite for LocalJsonOutput class."""

    async def test_init_creates_directory(self, temp_dir, basic_config):
        """Test that initialization creates the output directory."""
        output = LocalJsonOutput(basic_config)
        assert temp_dir.exists()
        assert temp_dir.is_dir()

    async def test_write_jsonl_format(self, temp_dir, basic_config, sample_data):
        """Test writing data in JSONL format."""
        output = LocalJsonOutput(basic_config)
        await output.write(sample_data)

        # Find the created file
        files = list(temp_dir.glob("*.jsonl"))
        assert len(files) == 1
        
        # Verify file contents
        with open(files[0], "r") as f:
            lines = f.readlines()
            assert len(lines) == len(sample_data)
            for line, expected in zip(lines, sample_data):
                assert json.loads(line.strip()) == expected

    async def test_write_json_format(self, temp_dir, sample_data):
        """Test writing data in regular JSON format."""
        config = OutputConfig(
            type="local_json",
            enabled=True,
            config={
                "output_dir": str(temp_dir),
                "file_format": "json"
            }
        )
        output = LocalJsonOutput(config)
        await output.write(sample_data)

        # Find the created file
        files = list(temp_dir.glob("*.json"))
        assert len(files) == 1
        
        # Verify file contents
        with open(files[0], "r") as f:
            data = json.load(f)
            assert data == sample_data

    async def test_single_partition(self, temp_dir, partitioned_config, sample_data):
        """Test writing with single field partitioning."""
        output = LocalJsonOutput(partitioned_config)
        await output.write(sample_data)

        # Verify partition structure
        partition_dir = temp_dir / "test-org"
        assert partition_dir.exists()
        assert partition_dir.is_dir()

        # Verify file contents
        files = list(partition_dir.glob("*.jsonl"))
        assert len(files) == 1
        with open(files[0], "r") as f:
            lines = f.readlines()
            assert len(lines) == len(sample_data)

    async def test_multi_partition(self, temp_dir, multi_partition_config, sample_data):
        """Test writing with multiple partition fields."""
        output = LocalJsonOutput(multi_partition_config)
        await output.write(sample_data)

        # Verify partition structure for first record
        partition_path = temp_dir / "test-org" / "5128581"
        assert partition_path.exists()
        assert partition_path.is_dir()

        # Verify file contents
        files = list(partition_path.glob("*.jsonl"))
        assert len(files) == 1

    async def test_date_partition(self, temp_dir, date_partition_config, sample_data):
        """Test writing with date-based partitioning."""
        output = LocalJsonOutput(date_partition_config)
        await output.write(sample_data)

        # Get expected date path
        date_path = datetime.now(UTC).strftime("%Y/%m/%d")
        partition_path = temp_dir.joinpath(*date_path.split("/"))
        
        assert partition_path.exists()
        assert partition_path.is_dir()

        # Verify file contents
        files = list(partition_path.glob("*.jsonl"))
        assert len(files) == 1

    async def test_empty_data(self, temp_dir, basic_config):
        """Test handling of empty data."""
        output = LocalJsonOutput(basic_config)
        await output.write([])

        # Verify no files were created
        files = list(temp_dir.glob("*.jsonl"))
        assert len(files) == 0

    async def test_missing_partition_field(self, temp_dir, sample_data):
        """Test handling of missing partition field."""
        config = OutputConfig(
            type="local_json",
            enabled=True,
            config={
                "output_dir": str(temp_dir),
                "partition_by": "non_existent_field"
            }
        )
        output = LocalJsonOutput(config)
        await output.write(sample_data)

        # Verify data is written to "unknown" partition
        partition_dir = temp_dir / "unknown"
        assert partition_dir.exists()
        assert partition_dir.is_dir()

    async def test_close_method(self, temp_dir, basic_config):
        """Test close method (should do nothing but not fail)."""
        output = LocalJsonOutput(basic_config)
        await output.close()
        # No assertions needed, just verify it doesn't raise exceptions

    async def test_invalid_output_dir(self, temp_dir):
        """Test handling of invalid output directory."""
        # Create a file where the directory should be
        file_path = temp_dir / "blocked"
        file_path.write_text("blocking file")

        config = OutputConfig(
            type="local_json",
            enabled=True,
            config={
                "output_dir": str(file_path),  # This is a file, not a directory
            }
        )

        with pytest.raises(Exception):
            LocalJsonOutput(config)

    async def test_write_permission_error(self, temp_dir, basic_config, sample_data, monkeypatch):
        """Test handling of write permission errors."""
        def mock_open(*args, **kwargs):
            raise PermissionError("Permission denied")

        output = LocalJsonOutput(basic_config)
        monkeypatch.setattr("builtins.open", mock_open)

        with pytest.raises(Exception) as exc_info:
            await output.write(sample_data)
        assert "Permission denied" in str(exc_info.value) 