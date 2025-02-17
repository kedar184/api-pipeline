import asyncio
import importlib
from typing import Dict, List, Any, Type, Optional
from datetime import datetime, UTC
from loguru import logger

from api_pipeline.core.base import BaseExtractor, ExtractorConfig
from api_pipeline.core.chain.models import (
    ChainConfig,
    ChainedExtractorConfig,
    ProcessingMode,
    ErrorHandlingMode,
    ResultMode
)
from api_pipeline.core.chain.state import ChainStateManager

class ChainExecutor:
    """Executes chains of extractors with configurable processing modes."""
    
    def __init__(self):
        """Initialize the chain executor."""
        self._metrics = {
            'chains_executed': 0,
            'chains_failed': 0,
            'total_execution_time': 0.0,
            'last_execution_time': None,
            'items_processed': 0,
            'errors_encountered': 0,
            'retries_performed': 0
        }
    
    def _import_extractor_class(self, class_path: str) -> Type[BaseExtractor]:
        """Import extractor class from string.
        
        Args:
            class_path: Fully qualified class path (e.g., 'package.module.ClassName')
            
        Returns:
            Extractor class
            
        Raises:
            ImportError: If class cannot be imported
            ValueError: If class is not a BaseExtractor
        """
        try:
            module_path, class_name = class_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            extractor_class = getattr(module, class_name)
            
            if not issubclass(extractor_class, BaseExtractor):
                raise ValueError(f"Class {class_path} is not a BaseExtractor")
                
            return extractor_class
        except Exception as e:
            logger.error(f"Error importing extractor class {class_path}: {str(e)}")
            raise
    
    async def execute_step(
        self,
        step_config: ChainedExtractorConfig,
        input_data: Any,
        state_manager: ChainStateManager
    ) -> Any:
        """Execute a single step based on its processing mode."""
        # Import and create extractor
        extractor_class = self._import_extractor_class(step_config.extractor_class)
        extractor = extractor_class(ExtractorConfig(**step_config.extractor_config))
        
        if step_config.processing_mode == ProcessingMode.SINGLE:
            return await self._execute_single(step_config, input_data, extractor)
            
        elif step_config.processing_mode == ProcessingMode.SEQUENTIAL:
            if not isinstance(input_data, dict) or not any(isinstance(v, list) for v in input_data.values()):
                raise ValueError("Sequential mode requires list input in one of the mapped fields")
            return await self._execute_sequential(
                step_config, input_data, extractor
            )
            
        elif step_config.processing_mode == ProcessingMode.PARALLEL:
            if not isinstance(input_data, dict) or not any(isinstance(v, list) for v in input_data.values()):
                raise ValueError("Parallel mode requires list input in one of the mapped fields")
            return await self._execute_parallel(
                step_config, input_data, extractor
            )
        
        raise ValueError(f"Unknown processing mode: {step_config.processing_mode}")

    async def _execute_single(
        self,
        config: ChainedExtractorConfig,
        data: Any,
        extractor: BaseExtractor
    ) -> Any:
        """Process single item directly."""
        params = self._map_input(data, config.input_mapping)
        try:
            result = await extractor.extract(params)
            self._metrics['items_processed'] += 1
            return result
        except Exception as e:
            self._metrics['errors_encountered'] += 1
            raise

    async def _execute_sequential(
        self,
        config: ChainedExtractorConfig,
        data: Dict[str, Any],
        extractor: BaseExtractor
    ) -> Dict[str, Any]:
        """Process list items sequentially with error handling."""
        sequential_config = config.sequential_config
        if not sequential_config:
            sequential_config = SequentialConfig()  # Use defaults
            
        # Find the list field in input data
        list_field = None
        list_items = None
        for field, items in data.items():
            if isinstance(items, list):
                list_field = field
                list_items = items
                break
                
        if not list_field or not list_items:
            raise ValueError("No list field found in input data")
            
        results = []
        errors = {}
        
        for item in list_items:
            try:
                # Create input data with current item
                item_data = {**data, list_field: item}
                result = await self._execute_single(config, item_data, extractor)
                results.append(result)
            except Exception as e:
                self._metrics['errors_encountered'] += 1
                if sequential_config.error_handling == ErrorHandlingMode.FAIL_FAST:
                    raise
                errors[str(item)] = {"error": str(e)}
                if sequential_config.error_handling == ErrorHandlingMode.CONTINUE:
                    continue
                
        # Format results based on configuration
        if sequential_config.result_handling == ResultMode.DICT:
            output = {
                "results": {str(item): result for item, result in zip(list_items, results)},
                "errors": errors if errors else None
            }
        else:
            output = {
                "results": results,
                "errors": errors if errors else None
            }
            
        return output

    async def _execute_parallel(
        self,
        config: ChainedExtractorConfig,
        data: Dict[str, Any],
        extractor: BaseExtractor
    ) -> Dict[str, Any]:
        """Process list items in parallel with configuration."""
        parallel_config = config.parallel_config
        if not parallel_config:
            raise ValueError("Parallel configuration required for parallel mode")
            
        # Find the list field in input data
        list_field = None
        list_items = None
        for field, items in data.items():
            if isinstance(items, list):
                list_field = field
                list_items = items
                break
                
        if not list_field or not list_items:
            raise ValueError("No list field found in input data")
            
        semaphore = asyncio.Semaphore(parallel_config.max_concurrent)
        
        async def process_with_semaphore(item):
            async with semaphore:
                try:
                    # Create input data with current item
                    item_data = {**data, list_field: item}
                    result = await self._execute_single(config, item_data, extractor)
                    return str(item), result, None
                except Exception as e:
                    self._metrics['errors_encountered'] += 1
                    if parallel_config.error_handling == ErrorHandlingMode.FAIL_FAST:
                        raise
                    return str(item), None, str(e)
        
        # Process items with optional batching
        if parallel_config.batch_size:
            all_results = []
            for i in range(0, len(list_items), parallel_config.batch_size):
                batch = list_items[i:i + parallel_config.batch_size]
                batch_results = await asyncio.gather(*[
                    process_with_semaphore(item) for item in batch
                ])
                all_results.extend(batch_results)
        else:
            all_results = await asyncio.gather(*[
                process_with_semaphore(item) for item in list_items
            ])
        
        # Organize results and errors
        results = {}
        errors = {}
        
        for item_key, result, error in all_results:
            if error:
                errors[item_key] = {"error": error}
            elif result is not None:
                results[item_key] = result
        
        # Format output based on configuration
        if parallel_config.result_handling == ResultMode.DICT:
            output = {
                "results": results,
                "errors": errors if errors else None
            }
        else:
            output = {
                "results": list(results.values()),
                "errors": errors if errors else None
            }
            
        return output

    def _map_input(self, data: Any, mapping: Dict[str, str]) -> Dict[str, Any]:
        """Map input data to extractor parameters."""
        if isinstance(data, dict):
            mapped = {}
            for param, state_key in mapping.items():
                value = data.get(state_key)
                if value is None:
                    logger.warning(f"No value found for state key: {state_key}")
                mapped[param] = value
            return mapped
        return {param: data for param, _ in mapping.items()}

    def _map_output(self, data: Any, mapping: Dict[str, str]) -> Dict[str, Any]:
        """Map extractor output to chain state.
        
        Args:
            data: Output data from the extractor
            mapping: Dictionary mapping result keys to state keys
            
        Returns:
            Dictionary of mapped state values
        """
        if not isinstance(data, dict):
            return {state_key: data for _, state_key in mapping.items()}
            
        mapped = {}
        for result_key, state_key in mapping.items():
            value = data.get(result_key)
            if value is None:
                logger.warning(f"No value found for result key: {result_key}")
            mapped[state_key] = value
            
        return mapped

    async def execute_single_chain(
        self,
        chain_config: ChainConfig,
        initial_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a single chain of extractors."""
        start_time = datetime.now(UTC)
        state_manager = ChainStateManager(chain_config.chain_id, chain_config.state_dir)
        
        try:
            # Store initial parameters
            for key, value in initial_params.items():
                logger.info(f"Setting initial state: {key} = {value}")
                await state_manager.set_state(key, value)
            
            # Execute each step
            for step_num, step_config in enumerate(chain_config.extractors, 1):
                logger.info(f"Executing chain step {step_num}: {step_config.extractor_class}")
                
                # Get input data from state
                input_data = {}
                for param_name, state_key in step_config.input_mapping.items():
                    value = await state_manager.get_state(state_key)
                    logger.info(f"Got state for {state_key}: {value}")
                    input_data[state_key] = value
                
                # Execute step with appropriate processing mode
                try:
                    results = await self.execute_step(step_config, input_data, state_manager)
                    logger.info(f"Step {step_num} results: {results}")
                except Exception as e:
                    logger.error(f"Error in chain step {step_num}: {str(e)}")
                    raise
                
                # Map results to state based on processing mode
                if step_config.processing_mode == ProcessingMode.SINGLE:
                    # For single mode, map results directly using output mapping
                    if isinstance(results, dict):
                        for result_key, state_key in step_config.output_mapping.items():
                            value = results.get(result_key)
                            if value is not None:
                                logger.info(f"Setting state for {state_key}: {value}")
                                await state_manager.set_state(state_key, value)
                            else:
                                logger.warning(f"No value found for result key: {result_key}")
                    else:
                        # If results is not a dict, store it directly for each output mapping
                        for _, state_key in step_config.output_mapping.items():
                            logger.info(f"Setting state for {state_key}: {results}")
                            await state_manager.set_state(state_key, results)
                else:
                    # Handle sequential/parallel processing results
                    if isinstance(results, dict) and "results" in results:
                        if results.get("errors"):
                            logger.warning(f"Step {step_num} errors: {results['errors']}")
                            await state_manager.set_state("_errors", results["errors"])
                        results = results["results"]
                    
                    # Handle list results
                    if isinstance(results, list):
                        # If we have a list of dictionaries, extract the mapped fields
                        if results and isinstance(results[0], dict):
                            mapped_results = {}
                            for result_key, state_key in step_config.output_mapping.items():
                                if result_key in results[0]:
                                    mapped_results[state_key] = [r[result_key] for r in results]
                                else:
                                    logger.warning(f"No value found for result key: {result_key}")
                            results = mapped_results
                    
                    # Store results in state
                    if isinstance(results, dict):
                        for result_key, state_key in step_config.output_mapping.items():
                            value = results.get(result_key)
                            if value is not None:
                                logger.info(f"Setting state for {state_key}: {value}")
                                await state_manager.set_state(state_key, value)
                            else:
                                logger.warning(f"No value found for result key: {result_key}")
                    else:
                        # If results is not a dict, store it directly for each output mapping
                        for _, state_key in step_config.output_mapping.items():
                            logger.info(f"Setting state for {state_key}: {results}")
                            await state_manager.set_state(state_key, results)
            
            # Get final results
            final_results = {}
            for step_config in chain_config.extractors:
                for state_key in step_config.output_mapping.values():
                    value = await state_manager.get_state(state_key)
                    logger.info(f"Final result for {state_key}: {value}")
                    final_results[state_key] = value
            
            # Add errors if any
            errors = await state_manager.get_state("_errors")
            if errors:
                final_results["_errors"] = errors
            
            # Update metrics
            self._metrics['chains_executed'] += 1
            execution_time = (datetime.now(UTC) - start_time).total_seconds()
            self._metrics['total_execution_time'] += execution_time
            self._metrics['last_execution_time'] = datetime.now(UTC)
            
            return final_results
            
        except Exception as e:
            self._metrics['chains_failed'] += 1
            raise
            
        finally:
            await state_manager.cleanup()
    
    async def execute_parallel_chains(
        self,
        chain_config: ChainConfig,
        input_items: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Execute multiple chains in parallel.
        
        Args:
            chain_config: Chain configuration
            input_items: List of initial parameters for each chain
            
        Returns:
            List of results from each chain
        """
        semaphore = asyncio.Semaphore(chain_config.max_parallel_chains)
        
        async def execute_with_semaphore(params: Dict[str, Any]) -> Dict[str, Any]:
            async with semaphore:
                unique_config = chain_config.copy()
                unique_config.chain_id = f"{chain_config.chain_id}_{id(params)}"
                return await self.execute_single_chain(unique_config, params)
        
        return await asyncio.gather(*[
            execute_with_semaphore(params)
            for params in input_items
        ])
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get executor metrics.
        
        Returns:
            Dictionary of metrics
        """
        metrics = self._metrics.copy()
        
        # Calculate derived metrics
        if metrics['chains_executed'] > 0:
            metrics['success_rate'] = (
                (metrics['chains_executed'] - metrics['chains_failed']) /
                metrics['chains_executed']
            )
            metrics['avg_execution_time'] = (
                metrics['total_execution_time'] /
                metrics['chains_executed']
            )
            metrics['error_rate'] = (
                metrics['errors_encountered'] /
                metrics['items_processed']
                if metrics['items_processed'] > 0 else 0
            )
            
        return metrics 