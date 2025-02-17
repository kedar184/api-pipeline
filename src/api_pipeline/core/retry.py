from typing import Dict, Any, List, Optional
import time
import random
import asyncio
from pydantic import BaseModel
from loguru import logger


class RetryConfig(BaseModel):
    """Configuration for retry mechanism with exponential backoff."""
    max_attempts: int = 3                      # Maximum number of retry attempts
    max_time: int = 30                         # Maximum total retry time in seconds
    base_delay: float = 1.0                    # Initial delay between retries
    max_delay: float = 10.0                    # Maximum delay between retries
    jitter: bool = True                        # Whether to add random jitter to delays
    retry_on: List[int] = [429, 503, 504]     # HTTP status codes to retry on


class RetryState:
    """Manages retry state and attempts for requests."""
    
    def __init__(self):
        self._state: Dict[str, Any] = {}
    
    def get_state(self, request_id: str) -> Dict[str, Any]:
        """Get or create retry state for a request."""
        if request_id not in self._state:
            self._state[request_id] = {
                'attempts': 0,
                'start_time': time.monotonic(),
                'last_error': None
            }
        return self._state[request_id]
    
    def clear_state(self, request_id: str) -> None:
        """Clear retry state for a request."""
        self._state.pop(request_id, None)
    
    def clear_all(self) -> None:
        """Clear all retry states."""
        self._state.clear()


class RetryHandler:
    """Handles retry logic with exponential backoff."""
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self.state = RetryState()
        self._metrics = {
            'retry_attempts': 0,
            'retry_successes': 0,
            'retry_failures': 0,
            'total_retry_time': 0.0,
            'max_retry_time': 0.0,
            'min_retry_time': float('inf'),
            'retry_rate': 0.0,
            'retry_success_rate': 0.0
        }
        self._start_time = time.monotonic()
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay with optional jitter."""
        delay = min(
            self.config.base_delay * (2 ** attempt),
            self.config.max_delay
        )
        if self.config.jitter:
            delay *= (1 + random.random())
        return delay
    
    def should_retry(self, status_code: int) -> bool:
        """Determine if request should be retried based on status code."""
        return (
            status_code in self.config.retry_on or
            status_code >= 500
        )
    
    async def execute_with_retry(self, request_id: str, func, *args, **kwargs) -> Any:
        """Execute a function with retry logic.
        
        Args:
            request_id: Unique identifier for the request
            func: Async function to execute
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func
            
        Returns:
            Result from the function execution
            
        Raises:
            Exception: If all retry attempts fail
        """
        state = self.state.get_state(request_id)
        retry_start_time = time.monotonic()
        
        while True:
            try:
                result = await func(*args, **kwargs)
                if state['attempts'] > 0:
                    self._metrics['retry_successes'] += 1
                    retry_time = time.monotonic() - retry_start_time
                    self._metrics['total_retry_time'] += retry_time
                    self._metrics['max_retry_time'] = max(self._metrics['max_retry_time'], retry_time)
                    self._metrics['min_retry_time'] = min(self._metrics['min_retry_time'], retry_time)
                return result
                
            except Exception as e:
                state['attempts'] += 1
                state['last_error'] = str(e)
                self._metrics['retry_attempts'] += 1
                
                # Check if we should retry
                if (
                    state['attempts'] < self.config.max_attempts and
                    time.monotonic() - state['start_time'] < self.config.max_time
                ):
                    delay = self.calculate_delay(state['attempts'])
                    logger.warning(
                        f"Request {request_id} failed (attempt {state['attempts']}). "
                        f"Retrying in {delay:.2f}s"
                    )
                    await asyncio.sleep(delay)
                    continue
                    
                # Clean up state and update failure metrics
                self.state.clear_state(request_id)
                self._metrics['retry_failures'] += 1
                retry_time = time.monotonic() - retry_start_time
                self._metrics['total_retry_time'] += retry_time
                self._metrics['max_retry_time'] = max(self._metrics['max_retry_time'], retry_time)
                self._metrics['min_retry_time'] = min(self._metrics['min_retry_time'], retry_time)
                raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current retry metrics."""
        current_time = time.monotonic()
        metrics = self._metrics.copy()
        uptime = current_time - self._start_time
        
        # Calculate derived metrics
        total_retries = metrics['retry_attempts']
        if total_retries > 0:
            metrics['retry_success_rate'] = metrics['retry_successes'] / total_retries
        
        if uptime > 0:
            metrics['retry_rate'] = total_retries / uptime
        
        if metrics['min_retry_time'] == float('inf'):
            metrics['min_retry_time'] = 0.0
        
        metrics['average_retry_time'] = (
            metrics['total_retry_time'] / total_retries
            if total_retries > 0 else 0.0
        )
        
        return metrics 