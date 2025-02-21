import asyncio
import importlib
from typing import Dict, List, Any, Type, Optional, Tuple
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
        state_manager: ChainStateManager,
        chain_config: ChainConfig
    ) -> Any:
        """Execute a single step based on its processing mode."""
        # Import and create extractor
        extractor_class = self._import_extractor_class(step_config.extractor_class)
        
        # Import the config class from the same module
        module_path, class_name = step_config.extractor_class.rsplit('.', 1)
        module = importlib.import_module(module_path)
        config_class = getattr(module, f"{class_name.replace('Extractor', '')}Config")
        
        # Get merged configuration from chain config
        extractor_name = next(
            name for name, config in chain_config.extractors.items()
            if config.extractor_class == step_config.extractor_class
        )
        merged_config = chain_config.get_extractor_config(extractor_name)
        
        # Create extractor with the correct config class
        extractor = extractor_class(config_class(**merged_config))
        
        logger.info(f"[Step {step_config.extractor_class}] Processing mode: {step_config.processing_mode}")
        logger.info(f"[Step {step_config.extractor_class}] Input data: {input_data}")
        logger.info(f"[Step {step_config.extractor_class}] Input mapping: {step_config.input_mapping}")
        
        if step_config.processing_mode == ProcessingMode.SINGLE:
            return await self._execute_single(step_config, input_data, extractor)
            
        elif step_config.processing_mode == ProcessingMode.SEQUENTIAL:
            if not isinstance(input_data, dict) or not any(isinstance(v, list) for v in input_data.values()):
                logger.error(f"[Step {step_config.extractor_class}] Sequential mode requires list input, got: {input_data}")
                raise ValueError("Sequential mode requires list input in one of the mapped fields")
            return await self._execute_sequential(
                step_config, input_data, extractor
            )
            
        elif step_config.processing_mode == ProcessingMode.PARALLEL:
            if not isinstance(input_data, dict) or not any(isinstance(v, list) for v in input_data.values()):
                logger.error(f"[Step {step_config.extractor_class}] Parallel mode requires list input, got: {input_data}")
                raise ValueError("Parallel mode requires list input in one of the mapped fields")
            return await self._execute_parallel(
                step_config, input_data, extractor
            )
        
        raise ValueError(f"Unknown processing mode: {step_config.processing_mode}")

    def _find_list_field(self, data: Dict[str, Any]) -> Tuple[Optional[str], Optional[List[Any]]]:
        """Find the first list field in input data."""
        for field, items in data.items():
            if isinstance(items, list):
                return field, items
        return None, None

    def _format_results(
        self,
        results: List[Dict[str, Any]],
        errors: Dict[str, Any],
        result_mode: ResultMode
    ) -> Dict[str, Any]:
        """Format results based on configuration."""
        if result_mode == ResultMode.DICT:
            output = {
                "results": results,
                "errors": errors if errors else None
            }
        else:  # LIST mode
            output = {
                "results": results,
                "errors": errors if errors else None
            }
        return output

    async def _execute_single(
        self,
        config: ChainedExtractorConfig,
        data: Any,
        extractor: BaseExtractor
    ) -> List[Dict[str, Any]]:
        """Process item(s) and always return a list of dictionaries."""
        params = self._map_input(data, config.input_mapping)
        try:
            results = await extractor.extract(params)
            # Ensure we always have a list of dictionaries
            if not results:
                return []
            if not isinstance(results, list):
                results = [results]
            self._metrics['items_processed'] += len(results)
            return results
        except Exception as e:
            self._metrics['errors_encountered'] += 1
            raise

    async def _process_item(
        self,
        item: Any,
        item_data: Dict[str, Any],
        extractor: BaseExtractor,
        error_mode: ErrorHandlingMode
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Process a single item with error handling."""
        try:
            result = await extractor.extract(item_data)
            if not isinstance(result, list):
                result = [result]
            self._metrics['items_processed'] += len(result)
            return result, None
        except Exception as e:
            self._metrics['errors_encountered'] += 1
            if error_mode == ErrorHandlingMode.FAIL_FAST:
                raise
            return [], {"error": str(e)}

    async def _execute_sequential(
        self,
        config: ChainedExtractorConfig,
        data: Dict[str, Any],
        extractor: BaseExtractor
    ) -> Dict[str, Any]:
        """Process list items sequentially with error handling."""
        sequential_config = config.sequential_config or SequentialConfig()
        
        # Find the list field in input data
        list_field, list_items = self._find_list_field(data)
        if not list_field or not list_items:
            raise ValueError("No list field found in input data")
            
        results = []
        errors = {}
        
        for item in list_items:
            # Create input data with just the current item and mapped field name
            item_data = {}
            for param_name, state_key in config.input_mapping.items():
                if state_key == list_field:
                    item_data[param_name] = item
                else:
                    item_data[param_name] = data.get(state_key)
            
            # Process item
            item_results, error = await self._process_item(
                item, 
                item_data, 
                extractor, 
                sequential_config.error_handling
            )
            
            if error:
                errors[str(item)] = error
                if sequential_config.error_handling == ErrorHandlingMode.CONTINUE:
                    continue
            else:
                results.extend(item_results)
                
        return self._format_results(results, errors, sequential_config.result_handling)

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
        list_field, list_items = self._find_list_field(data)
        if not list_field or not list_items:
            raise ValueError("No list field found in input data")
            
        semaphore = asyncio.Semaphore(parallel_config.max_concurrent)
        
        async def process_with_semaphore(item):
            async with semaphore:
                # Create input data with current item
                item_data = {}
                for param_name, state_key in config.input_mapping.items():
                    if state_key == list_field:
                        item_data[param_name] = item
                    else:
                        item_data[param_name] = data.get(state_key)
                
                # Process item
                item_results, error = await self._process_item(
                    item,
                    item_data,
                    extractor,
                    parallel_config.error_handling
                )
                return str(item), item_results, error
        
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
        results = []
        errors = {}
        
        for item_key, item_results, error in all_results:
            if error:
                errors[item_key] = error
            else:
                results.extend(item_results)
        
        return self._format_results(results, errors, parallel_config.result_handling)

    def _map_input(self, data: Any, mapping: Dict[str, str]) -> Dict[str, Any]:
        """Map input data to extractor parameters."""
        if isinstance(data, dict):
            mapped = {}
            for param, state_key in mapping.items():
                value = data.get(state_key)
                if value is None:
                    logger.warning(f"No value found for state key: {state_key}")
                mapped[param] = value
            logger.info(f"Mapped input data: {mapped}")
            return mapped
        return {param: data for param, _ in mapping.items()}

    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """Get value from nested dictionary using dot notation"""
        if path == 'self':
            return data
        
        keys = path.split('.')
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value

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
            logger.info(f"[Chain {chain_config.chain_id}] Starting chain execution")
            logger.info(f"[Chain {chain_config.chain_id}] Initial parameters: {initial_params}")
            
            for key, value in initial_params.items():
                logger.info(f"[Chain {chain_config.chain_id}] Setting initial state: {key} = {value}")
                await state_manager.set_state(key, value)
            
            # Execute each step
            for step_num, step in enumerate(chain_config.chain, 1):
                # Get extractor config
                extractor_config = chain_config.extractors[step.extractor]
                logger.info(f"[Chain {chain_config.chain_id}] Executing step {step_num}: {extractor_config.extractor_class}")
                logger.info(f"[Chain {chain_config.chain_id}] Step {step_num} processing mode: {extractor_config.processing_mode}")
                
                # Check condition if present
                if step.condition:
                    field = step.condition['field']
                    operator = step.condition['operator']
                    value = step.condition['value']
                    
                    field_value = await state_manager.get_state(field)
                    if field_value is None:
                        logger.warning(f"[Chain {chain_config.chain_id}] Step {step_num} condition field not found: {field}")
                        continue
                    
                    # Handle empty list results
                    if isinstance(field_value, list):
                        if not field_value:  # Empty list
                            logger.warning(f"[Chain {chain_config.chain_id}] Step {step_num} condition field is empty list: {field}")
                            continue
                        field_value = field_value[0]  # Take first value for comparison
                    
                    # Convert to float for numeric comparisons
                    try:
                        field_value = float(field_value)
                    except (TypeError, ValueError):
                        logger.warning(f"[Chain {chain_config.chain_id}] Step {step_num} condition field not numeric: {field}")
                        continue
                    
                    if operator == 'gt' and not (field_value > value):
                        logger.info(f"[Chain {chain_config.chain_id}] Step {step_num} condition not met: {field_value} <= {value}")
                        continue
                    elif operator == 'lt' and not (field_value < value):
                        logger.info(f"[Chain {chain_config.chain_id}] Step {step_num} condition not met: {field_value} >= {value}")
                        continue
                    elif operator == 'eq' and not (field_value == value):
                        logger.info(f"[Chain {chain_config.chain_id}] Step {step_num} condition not met: {field_value} != {value}")
                        continue
                
                # Get input parameters from state
                input_params = {}
                for state_key, param_key in extractor_config.input_mapping.items():
                    value = await state_manager.get_state(state_key)
                    if value is not None:
                        logger.info(f"[Chain {chain_config.chain_id}] Step {step_num} input mapping: {state_key} -> {param_key} = {value}")
                        input_params[param_key] = value
                
                # Execute step
                try:
                    results = await self.execute_step(extractor_config, input_params, state_manager, chain_config)
                    logger.info(f"[Chain {chain_config.chain_id}] Step {step_num} raw results: {results}")
                    
                    # Map results to state
                    if results:
                        for result_key, state_key in extractor_config.output_mapping.items():
                            value = self._get_nested_value(results[0], result_key)
                            if value is not None:
                                logger.info(f"[Chain {chain_config.chain_id}] Step {step_num} setting state: {state_key} = {value}")
                                await state_manager.set_state(state_key, value)
                            else:
                                logger.warning(f"[Chain {chain_config.chain_id}] Step {step_num} no value for result key: {result_key}")
                    else:
                        logger.warning(f"[Chain {chain_config.chain_id}] Step {step_num} produced no results")
                        
                except Exception as e:
                    if extractor_config.required:
                        raise
                    logger.warning(f"[Chain {chain_config.chain_id}] Non-required step {step_num} failed: {str(e)}")
            
            # Get final state
            final_state = await state_manager.get_all_state()
            logger.info(f"[Chain {chain_config.chain_id}] Chain execution completed in {(datetime.now(UTC) - start_time).total_seconds():.2f}s")
            return final_state
            
        except Exception as e:
            logger.error(f"[Chain {chain_config.chain_id}] Chain execution failed: {str(e)}")
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