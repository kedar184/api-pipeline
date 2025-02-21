from pathlib import Path
import json
import asyncio
from typing import Any, Optional, Dict
from loguru import logger
from datetime import datetime
import os
import shutil
import aiofiles

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class ChainStateManager:
    """Manages state between chain steps.
    
    This class handles:
    - State storage and retrieval
    - State cleanup
    - Metrics tracking
    """
    
    def __init__(self, chain_id: str, state_dir: str = "./chain_state"):
        """Initialize the state manager.
        
        Args:
            chain_id: Unique identifier for the chain
            state_dir: Base directory for state files
        """
        self.chain_id = chain_id
        self.state_dir = os.path.join(state_dir, f"{chain_id}_{id(self)}")
        os.makedirs(self.state_dir, exist_ok=True)
        self._metrics = {
            'gets': 0,
            'sets': 0,
            'bytes_written': 0,
            'bytes_read': 0
        }
        self._lock = asyncio.Lock()
    
    def _get_state_path(self, key: str) -> str:
        """Get the file path for a state key.
        
        Args:
            key: State key
            
        Returns:
            Path to state file
        """
        # Ensure safe file naming
        safe_key = "".join(c if c.isalnum() else "_" for c in key)
        return os.path.join(self.state_dir, f"{safe_key}.json")
    
    async def get_state(self, key: str) -> Optional[Any]:
        """Read state from file.
        
        Args:
            key: State key to read
            
        Returns:
            State value if exists, None otherwise
        """
        self._metrics['gets'] += 1
        path = self._get_state_path(key)
        
        if not os.path.exists(path):
            logger.debug(f"No state found for key: {key}")
            return None
            
        try:
            async with self._lock:
                with open(path, 'r') as f:
                    data = f.read()
                    self._metrics['bytes_read'] += len(data)
                    return json.loads(data)
        except Exception as e:
            logger.error(f"Error reading state for key {key}: {str(e)}")
            return None
    
    async def set_state(self, key: str, value: Any) -> None:
        """Write state to file.
        
        Args:
            key: State key to write
            value: Value to store
        """
        self._metrics['sets'] += 1
        path = self._get_state_path(key)
        
        try:
            async with self._lock:
                data = json.dumps(value, cls=DateTimeEncoder)
                with open(path, 'w') as f:
                    f.write(data)
                self._metrics['bytes_written'] += len(data)
                logger.debug(f"Stored state for key: {key}")
        except Exception as e:
            logger.error(f"Error writing state for key {key}: {str(e)}")
            raise
    
    async def get_all_state(self) -> Dict[str, Any]:
        """Get all stored state values.

        Returns:
            A dictionary containing all state key-value pairs
        """
        try:
            state = {}
            for root, _, files in os.walk(self.state_dir):
                for file in files:
                    if file.endswith('.json'):
                        # Get relative path from state directory
                        rel_path = os.path.relpath(root, self.state_dir)
                        if rel_path == '.':
                            key = os.path.splitext(file)[0]
                        else:
                            key = f"{rel_path}.{os.path.splitext(file)[0]}"
                        
                        file_path = os.path.join(root, file)
                        async with aiofiles.open(file_path, 'r') as f:
                            data = await f.read()
                            state[key] = json.loads(data)
            return state
        except Exception as e:
            logger.error(f"Error getting all state: {str(e)}")
            return {}
    
    async def cleanup(self) -> None:
        """Clean up state files."""
        try:
            if os.path.exists(self.state_dir):
                shutil.rmtree(self.state_dir)
                logger.info(f"Cleaned up state directory: {self.state_dir}")
        except Exception as e:
            logger.error(f"Error cleaning up state directory: {str(e)}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get state manager metrics.
        
        Returns:
            Dictionary of metrics
        """
        metrics = self._metrics.copy()
        
        # Calculate derived metrics
        if metrics['sets'] > 0:
            metrics['avg_bytes_per_write'] = metrics['bytes_written'] / metrics['sets']
        if metrics['gets'] > 0:
            metrics['avg_bytes_per_read'] = metrics['bytes_read'] / metrics['gets']
            
        return metrics 