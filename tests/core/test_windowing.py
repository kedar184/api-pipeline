import pytest
from datetime import datetime, timedelta, UTC
from typing import Dict, List, Any, Optional, Tuple

from api_pipeline.core.windowing import (
    WindowConfig,
    WatermarkConfig,
    TimeWindowParallelConfig,
    WindowType,
    calculate_window_bounds,
    calculate_time_windows
)

class TestWindowConfig:
    """Test suite for window configuration."""
    
    def test_window_size_parsing(self):
        """Test parsing of window size strings."""
        test_cases = [
            ("1s", 1),
            ("5m", 300),
            ("2h", 7200),
            ("1d", 86400)
        ]
        
        for duration_str, expected_seconds in test_cases:
            config = WindowConfig(
                window_type=WindowType.FIXED,
                window_size=duration_str
            )
            assert config.window_size_seconds == expected_seconds
    
    def test_window_overlap_parsing(self):
        """Test parsing of window overlap strings."""
        config = WindowConfig(
            window_type=WindowType.SLIDING,
            window_size="1h",
            window_overlap="15m"
        )
        
        assert config.window_overlap_seconds == 900  # 15 minutes
    
    def test_invalid_duration_format(self):
        """Test handling of invalid duration formats."""
        with pytest.raises(ValueError):
            WindowConfig(
                window_type=WindowType.FIXED,
                window_size="invalid"
            )
        
        with pytest.raises(ValueError):
            WindowConfig(
                window_type=WindowType.FIXED,
                window_size="1h",
                window_overlap="invalid"
            )

class TestWatermarkConfig:
    """Test suite for watermark configuration."""
    
    def test_lookback_window_parsing(self):
        """Test parsing of lookback window duration."""
        config = WatermarkConfig(
            enabled=True,
            timestamp_field="updated_at",
            lookback_window="2h"
        )
        
        assert config.lookback_seconds == 7200  # 2 hours
    
    def test_default_values(self):
        """Test default configuration values."""
        config = WatermarkConfig(
            enabled=True,
            timestamp_field="updated_at"
        )
        
        assert config.lookback_seconds == 0
        assert config.time_format == "%Y-%m-%dT%H:%M:%SZ"
        assert config.start_time_param == "start_time"
        assert config.end_time_param == "end_time"
    
    def test_window_configuration(self):
        """Test watermark window configuration."""
        config = WatermarkConfig(
            enabled=True,
            timestamp_field="updated_at",
            window=WindowConfig(
                window_type=WindowType.FIXED,
                window_size="1h"
            )
        )
        
        assert config.window is not None
        assert config.window.window_type == WindowType.FIXED
        assert config.window.window_size_seconds == 3600

class TestWindowBoundsCalculation:
    """Test suite for window bounds calculation."""
    
    def test_fixed_windows(self):
        """Test calculation of fixed window bounds."""
        start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 4, 0, tzinfo=UTC)
        
        config = WindowConfig(
            window_type=WindowType.FIXED,
            window_size="1h"
        )
        
        windows = calculate_window_bounds(start_time, end_time, config)
        
        assert len(windows) == 4
        for i, (window_start, window_end) in enumerate(windows):
            expected_start = start_time + timedelta(hours=i)
            expected_end = start_time + timedelta(hours=i+1)
            assert window_start == expected_start
            assert window_end == expected_end
    
    def test_sliding_windows(self):
        """Test calculation of sliding window bounds."""
        start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 2, 0, tzinfo=UTC)
        
        config = WindowConfig(
            window_type=WindowType.SLIDING,
            window_size="1h",
            window_overlap="30m"
        )
        
        windows = calculate_window_bounds(start_time, end_time, config)
        
        # Should have 3 windows with 30-minute overlap
        assert len(windows) == 3
        
        # Verify window boundaries
        window_starts = [w[0] for w in windows]
        window_ends = [w[1] for w in windows]
        
        # Check window starts are 30 minutes apart
        for i in range(1, len(window_starts)):
            time_diff = window_starts[i] - window_starts[i-1]
            assert time_diff == timedelta(minutes=30)
        
        # Check each window is 1 hour long
        for start, end in windows:
            assert end - start == timedelta(hours=1)
    
    def test_window_alignment(self):
        """Test that windows are properly aligned."""
        start_time = datetime(2024, 1, 1, 0, 30, tzinfo=UTC)  # Non-hour aligned
        end_time = datetime(2024, 1, 1, 2, 30, tzinfo=UTC)
        
        config = WindowConfig(
            window_type=WindowType.FIXED,
            window_size="1h"
        )
        
        windows = calculate_window_bounds(start_time, end_time, config)
        
        # Should still create proper 1-hour windows
        assert len(windows) == 2
        for start, end in windows:
            window_duration = end - start
            assert window_duration == timedelta(hours=1)
    
    def test_partial_windows(self):
        """Test handling of partial windows at boundaries."""
        start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 1, 30, tzinfo=UTC)  # 1.5 hours
        
        config = WindowConfig(
            window_type=WindowType.FIXED,
            window_size="1h"
        )
        
        windows = calculate_window_bounds(start_time, end_time, config)
        
        # Should have 2 windows, last one partial
        assert len(windows) == 2
        last_window_duration = windows[-1][1] - windows[-1][0]
        assert last_window_duration == timedelta(minutes=30)

class TestTimeWindowParallelConfig:
    """Test suite for time window parallel configuration."""
    
    def test_parallel_window_calculation(self):
        """Test calculation of parallel processing windows."""
        start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 4, 0, tzinfo=UTC)
        
        config = TimeWindowParallelConfig(
            window_size="1h",
            window_overlap="15m",
            max_concurrent_windows=2
        )
        
        windows = calculate_time_windows(start_time, end_time, config)
        
        # Verify window properties
        assert len(windows) > 0
        for start, end in windows:
            # Each window should be 1 hour
            assert end - start == timedelta(hours=1)
            
            # Windows should overlap by 15 minutes
            if windows.index((start, end)) > 0:
                prev_end = windows[windows.index((start, end)) - 1][1]
                overlap = prev_end - start
                assert overlap == timedelta(minutes=15)
    
    def test_max_concurrent_windows(self):
        """Test enforcement of maximum concurrent windows."""
        start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 8, 0, tzinfo=UTC)
        
        config = TimeWindowParallelConfig(
            window_size="1h",
            window_overlap="0m",
            max_concurrent_windows=4
        )
        
        windows = calculate_time_windows(start_time, end_time, config)
        
        # At any point, no more than max_concurrent_windows should overlap
        for i in range(len(windows)):
            current_window_start, current_window_end = windows[i]
            concurrent_count = sum(
                1 for j in range(len(windows))
                if i != j and self._windows_overlap(
                    windows[i],
                    windows[j]
                )
            )
            assert concurrent_count < config.max_concurrent_windows
    
    @staticmethod
    def _windows_overlap(window1: Tuple[datetime, datetime], 
                        window2: Tuple[datetime, datetime]) -> bool:
        """Helper method to check if two windows overlap."""
        start1, end1 = window1
        start2, end2 = window2
        return (start1 < end2) and (end1 > start2) 