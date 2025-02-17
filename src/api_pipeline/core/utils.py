from datetime import datetime, UTC
from typing import Union

def parse_datetime(dt_str: Union[str, datetime]) -> datetime:
    """Parse a datetime string into a datetime object.
    
    Args:
        dt_str: Datetime string or datetime object
        
    Returns:
        Parsed datetime object with UTC timezone
        
    Raises:
        ValueError: If the datetime string cannot be parsed
    """
    if isinstance(dt_str, datetime):
        return dt_str.replace(tzinfo=UTC)
        
    try:
        # Try ISO format first
        return datetime.fromisoformat(dt_str).replace(tzinfo=UTC)
    except ValueError:
        try:
            # Try common GitHub format
            return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            # Try other common formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d"
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(dt_str, fmt).replace(tzinfo=UTC)
                except ValueError:
                    continue
                    
            raise ValueError(f"Could not parse datetime string: {dt_str}") 