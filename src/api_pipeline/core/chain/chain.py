from typing import Dict, Any, List, Optional, Callable
from loguru import logger

from api_pipeline.core.base import BaseExtractor

class ExtractorChain:
    """Chain multiple extractors together with conditional execution and data transformation."""
    
    def __init__(self):
        self.extractors = []
        self._metrics = {
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'total_execution_time': 0.0,
            'items_processed': 0,
            'conditions_evaluated': 0,
            'conditions_passed': 0,
            'conditions_failed': 0,
            'transformations_applied': 0
        }
    
    def add_extractor(
        self,
        extractor: BaseExtractor,
        condition: Optional[Callable[[Dict[str, Any]], bool]] = None,
        output_transform: Optional[Callable[[List[Dict[str, Any]]], Dict[str, Any]]] = None
    ) -> None:
        """Add an extractor to the chain with optional condition and transformation.
        
        Args:
            extractor: The extractor to add
            condition: Optional function that takes the current state and returns bool
            output_transform: Optional function to transform extractor output
        """
        self.extractors.append({
            'extractor': extractor,
            'condition': condition,
            'output_transform': output_transform
        })
        logger.info(f"Added extractor to chain: {extractor.__class__.__name__}")
        if condition:
            logger.info("With condition function")
        if output_transform:
            logger.info("With output transform function")
    
    async def execute(self, initial_params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the chain of extractors.
        
        Args:
            initial_params: Initial parameters for the first extractor
            
        Returns:
            Combined results from all extractors
        """
        self._metrics['total_executions'] += 1
        state = initial_params.copy()
        
        try:
            for step in self.extractors:
                extractor = step['extractor']
                condition = step['condition']
                transform = step['output_transform']
                
                # Check condition if present
                if condition:
                    self._metrics['conditions_evaluated'] += 1
                    if not condition(state):
                        self._metrics['conditions_failed'] += 1
                        logger.info(f"Skipping {extractor.__class__.__name__} (condition not met)")
                        continue
                    self._metrics['conditions_passed'] += 1
                
                # Execute extractor
                logger.info(f"Executing {extractor.__class__.__name__}")
                try:
                    results = await extractor.extract(state)
                    self._metrics['items_processed'] += len(results)
                    
                    # Transform results if needed
                    if transform:
                        self._metrics['transformations_applied'] += 1
                        transformed = transform(results)
                        logger.info(f"Transformed results: {transformed}")
                        state.update(transformed)
                    else:
                        state.update(results[0] if results else {})
                        
                except Exception as e:
                    logger.error(f"Error in {extractor.__class__.__name__}: {str(e)}")
                    raise
            
            self._metrics['successful_executions'] += 1
            return state
            
        except Exception as e:
            self._metrics['failed_executions'] += 1
            raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get chain execution metrics."""
        metrics = self._metrics.copy()
        
        # Add derived metrics
        if metrics['total_executions'] > 0:
            metrics['success_rate'] = (
                metrics['successful_executions'] / metrics['total_executions']
            )
        if metrics['conditions_evaluated'] > 0:
            metrics['condition_pass_rate'] = (
                metrics['conditions_passed'] / metrics['conditions_evaluated']
            )
        
        # Add metrics from each extractor
        for step in self.extractors:
            extractor = step['extractor']
            extractor_metrics = extractor.get_metrics()
            metrics[f"{extractor.__class__.__name__}_metrics"] = extractor_metrics
        
        return metrics 