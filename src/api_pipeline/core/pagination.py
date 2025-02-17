from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union
from pydantic import BaseModel
from urllib.parse import urlparse, parse_qs

# Configuration classes for different pagination types
class PageNumberConfig(BaseModel):
    # Configuration for page number based pagination (e.g., ?page=1&per_page=100)
    page_param: str = "page"                    # Parameter name for page number
    size_param: str = "per_page"                # Parameter name for page size
    has_more_field: Optional[str] = None        # Response field indicating more pages
    total_pages_field: Optional[str] = None     # Response field with total pages


class CursorConfig(BaseModel):
    # Configuration for cursor-based pagination (e.g., ?cursor=abc123)
    cursor_param: str = "cursor"                # Parameter name in request URL
    cursor_field: str = "next_cursor"           # Field name in response JSON
    has_more_field: Optional[str] = None        # Response field indicating more pages


class OffsetConfig(BaseModel):
    # Configuration for offset-based pagination (e.g., ?offset=100&limit=50)
    offset_param: str = "offset"                # Parameter name for offset
    limit_param: str = "limit"                  # Parameter name for limit
    total_count_field: Optional[str] = None     # Response field with total count


class LinkHeaderConfig(BaseModel):
    # Configuration for Link header based pagination (like GitHub)
    rel_next: str = "next"                      # Link relation for next page
    parse_params: bool = True                   # Whether to parse URL params from next link


PaginationStrategyConfig = Union[
    PageNumberConfig,
    CursorConfig,
    OffsetConfig,
    LinkHeaderConfig
]


# Strategy implementations
class PaginationStrategy(ABC):
    # Abstract base class defining interface for different pagination strategies
    
    @abstractmethod
    async def get_next_page_params(self, 
        current_params: Dict[str, Any],
        response: Dict[str, Any],
        page_number: int
    ) -> Optional[Dict[str, Any]]:
        # Get parameters for next page based on current response
        # Args: current_params (request params), response (current page), page_number (1-based)
        # Returns: Parameters for next page, or None if no more pages
        pass
    
    @abstractmethod
    def get_initial_params(self, base_params: Dict[str, Any], page_size: int) -> Dict[str, Any]:
        # Get parameters for the first page
        # Args: base_params (base request params), page_size (items per page)
        # Returns: Parameters for first page
        pass


class PageNumberStrategy(PaginationStrategy):
    # Strategy for page number based pagination (e.g., ?page=1&per_page=100)
    
    def __init__(self, config: PageNumberConfig):
        self.config = config
    
    async def get_next_page_params(self, 
        current_params: Dict[str, Any],
        response: Dict[str, Any],
        page_number: int
    ) -> Optional[Dict[str, Any]]:
        # If we got a full page, there might be more
        items = response if isinstance(response, list) else []
        if len(items) >= current_params[self.config.size_param]:
            params = current_params.copy()
            params[self.config.page_param] = page_number + 1
            return params
        return None
    
    def get_initial_params(self, base_params: Dict[str, Any], page_size: int) -> Dict[str, Any]:
        params = base_params.copy()
        params[self.config.page_param] = 1
        params[self.config.size_param] = page_size
        return params


class CursorStrategy(PaginationStrategy):
    # Strategy for cursor-based pagination where each response includes a token/cursor for next page
    # Example usages:
    # - API using "next_token" param and "nextPageToken" in response
    # - API using standard cursor naming (cursor/next_cursor)
    
    def __init__(self, config: CursorConfig):
        self.config = config
    
    async def get_next_page_params(self,
        current_params: Dict[str, Any],
        response: Dict[str, Any],
        page_number: int
    ) -> Optional[Dict[str, Any]]:
        # Extract next cursor from response and add to params for next request
        next_cursor = response.get(self.config.cursor_field)
        if next_cursor:
            params = current_params.copy()
            params[self.config.cursor_param] = next_cursor
            return params
        return None
    
    def get_initial_params(self, base_params: Dict[str, Any], page_size: int) -> Dict[str, Any]:
        # No cursor needed for first page, just return base params
        return base_params.copy()


class LinkHeaderStrategy(PaginationStrategy):
    # Strategy for Link header based pagination (like GitHub API)
    
    def __init__(self, config: LinkHeaderConfig):
        self.config = config
    
    async def get_next_page_params(self,
        current_params: Dict[str, Any],
        response: Dict[str, Any],
        page_number: int
    ) -> Optional[Dict[str, Any]]:
        # Extract and parse next link from Link header
        if not isinstance(response, dict) or 'headers' not in response:
            return None
            
        links = response['headers'].get('Link', '')
        if not links:
            return None
            
        for link in links.split(','):
            if f'rel="{self.config.rel_next}"' in link:
                url = link.split(';')[0].strip()[1:-1]
                if not self.config.parse_params:
                    return {'url': url}
                    
                # Parse URL params into dict
                parsed = urlparse(url)
                params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                
                # Preserve essential parameters from current params
                essential_params = ['repo', 'since', 'until']
                for param in essential_params:
                    if param in current_params and param not in params:
                        params[param] = current_params[param]
                
                # Ensure page number increments correctly
                if 'page' in params:
                    params['page'] = str(page_number + 1)
                
                return params
                
        return None
    
    def get_initial_params(self, base_params: Dict[str, Any], page_size: int) -> Dict[str, Any]:
        # Add page size to base params if specified
        params = base_params.copy()
        if page_size:
            params['per_page'] = page_size
        return params


class OffsetStrategy(PaginationStrategy):
    # Strategy for offset-based pagination (e.g., ?offset=100&limit=50)
    
    def __init__(self, config: OffsetConfig):
        self.config = config
    
    async def get_next_page_params(self,
        current_params: Dict[str, Any],
        response: Dict[str, Any],
        page_number: int
    ) -> Optional[Dict[str, Any]]:
        # Calculate next offset based on current offset and limit
        current_offset = current_params.get(self.config.offset_param, 0)
        limit = current_params[self.config.limit_param]
        items = response if isinstance(response, list) else []
        
        if len(items) >= limit:
            params = current_params.copy()
            params[self.config.offset_param] = current_offset + limit
            return params
        return None
    
    def get_initial_params(self, base_params: Dict[str, Any], page_size: int) -> Dict[str, Any]:
        # Set initial offset to 0 and limit to page_size
        params = base_params.copy()
        params[self.config.offset_param] = 0
        params[self.config.limit_param] = page_size
        return params


class PaginationConfig(BaseModel):
    # Enhanced pagination configuration using strategy pattern for different pagination types
    model_config = {
        'arbitrary_types_allowed': True
    }
    
    enabled: bool = True                        # Whether pagination is enabled
    strategy: PaginationStrategy               # Strategy implementation to use
    strategy_config: PaginationStrategyConfig  # Configuration for the strategy
    page_size: int = 100                       # Number of items per page
    max_pages: Optional[int] = None            # Maximum number of pages to fetch
    
    @classmethod
    def with_page_numbers(cls, page_size: int = 100, **kwargs) -> 'PaginationConfig':
        # Factory method for page number pagination
        config = PageNumberConfig(**kwargs)
        return cls(
            strategy=PageNumberStrategy(config),
            strategy_config=config,
            page_size=page_size
        )
    
    @classmethod
    def with_cursor(cls, page_size: int = 100, **kwargs) -> 'PaginationConfig':
        # Factory method for cursor-based pagination
        config = CursorConfig(**kwargs)
        return cls(
            strategy=CursorStrategy(config),
            strategy_config=config,
            page_size=page_size
        )
    
    @classmethod
    def with_offset(cls, page_size: int = 100, **kwargs) -> 'PaginationConfig':
        # Factory method for offset-based pagination
        config = OffsetConfig(**kwargs)
        return cls(
            strategy=OffsetStrategy(config),
            strategy_config=config,
            page_size=page_size
        )
    
    @classmethod
    def with_link_header(cls, page_size: int = 100, **kwargs) -> 'PaginationConfig':
        # Factory method for Link header pagination
        config = LinkHeaderConfig(**kwargs)
        return cls(
            strategy=LinkHeaderStrategy(config),
            strategy_config=config,
            page_size=page_size
        ) 