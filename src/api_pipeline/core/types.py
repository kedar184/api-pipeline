"""Core type definitions."""
from enum import Enum, auto
from typing import Dict, Any, Optional, List, Union

class ProcessingPattern(str, Enum):
    """How to process items from the API."""
    # BATCH: Each item needs its own API call (e.g., /api/user/{id})
    # SINGLE: One request returns multiple items (e.g., /api/users?page=1)
    BATCH = "batch"
    SINGLE = "single"


class PaginationType(str, Enum):
    """Supported pagination types."""
    # Supported pagination types with example URL formats
    PAGE_NUMBER = "page_number"  # e.g., ?page=1&per_page=100
    CURSOR = "cursor"           # e.g., ?cursor=abc123
    OFFSET = "offset"           # e.g., ?offset=100&limit=50
    TOKEN = "token"             # e.g., ?page_token=xyz789
    LINK = "link"               # Uses Link headers (like GitHub)


class ParallelProcessingStrategy(str, Enum):
    """Strategies for parallel processing."""
    NONE = "none"
    TIME_WINDOWS = "time_windows"
    KNOWN_PAGES = "known_pages"
    CALCULATED_OFFSETS = "calculated_offsets"


class WindowType(str, Enum):
    """Types of windowing for data extraction."""
    # Window types for batch processing with time-based windows
    FIXED = "fixed"      # Fixed-size time windows
    SLIDING = "sliding"  # Sliding windows with overlap
    SESSION = "session"  # Session-based windows
    DYNAMIC = "dynamic"  # Dynamic size based on data
    ADAPTIVE = "adaptive"  # Adapts size based on rate


class AuthType:
    """Constants for supported authentication types."""
    OAUTH = "oauth"
    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"


__all__ = [
    'ProcessingPattern',
    'PaginationType',
    'ParallelProcessingStrategy',
    'WindowType',
    'AuthType'
] 