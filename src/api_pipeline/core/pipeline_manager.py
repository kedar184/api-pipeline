from datetime import datetime, UTC
import importlib
from typing import Any, Dict, List, Optional

import yaml
from loguru import logger

from api_pipeline.core.models import PipelineConfig, PipelineRun
from api_pipeline.core.factory import PipelineFactory, load_pipeline_config


class PipelineManager:
    def __init__(self):
        self.runs: Dict[str, PipelineRun] = {}
        self._pipeline_cache: Dict[str, PipelineConfig] = {}
    
    def _get_pipeline_config(self, pipeline_id: str) -> PipelineConfig:
        if pipeline_id not in self._pipeline_cache:
            try:
                config_dict = load_pipeline_config(pipeline_id)
                self._pipeline_cache[pipeline_id] = PipelineConfig(**config_dict)
            except Exception as e:
                logger.error(f"Failed to load pipeline {pipeline_id}: {str(e)}")
                raise ValueError(f"Pipeline {pipeline_id} not found or invalid")
        return self._pipeline_cache[pipeline_id]
    
    def _get_extractor_class(self, class_path: str):
        try:
            module_path, class_name = class_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        except Exception as e:
            logger.error(f"Failed to load extractor class {class_path}: {str(e)}")
            raise ImportError(f"Could not import {class_path}")
    
    def list_pipelines(self) -> List[Dict[str, Any]]:
        import os
        import glob

        env = os.getenv('ENVIRONMENT', 'dev')
        config_dir = os.path.join('config', env, 'sources')
        pipeline_files = glob.glob(os.path.join(config_dir, '*.yaml'))
        
        pipelines = []
        for file_path in pipeline_files:
            try:
                with open(file_path, 'r') as f:
                    config = yaml.safe_load(f)
                    pipelines.append({
                        "pipeline_id": config["pipeline_id"],
                        "description": config.get("description", ""),
                        "enabled": config.get("enabled", True)
                    })
            except Exception as e:
                logger.warning(f"Failed to load pipeline from {file_path}: {str(e)}")
                continue
        
        return pipelines
    
    def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        # Load config to verify pipeline exists
        self._get_pipeline_config(pipeline_id)
            
        # Get the most recent run for this pipeline
        pipeline_runs = [
            run for run in self.runs.values()
            if run.pipeline_id == pipeline_id
        ]
        latest_run = max(pipeline_runs, key=lambda x: x.start_time) if pipeline_runs else None
        
        return {
            "pipeline_id": pipeline_id,
            "status": latest_run.status if latest_run else "NEVER_RUN",
            "last_run": latest_run.start_time.isoformat() if latest_run else None,
            "records_processed": latest_run.records_processed if latest_run else 0,
            "errors": latest_run.errors if latest_run else []
        }
    
    async def execute_pipeline(self, pipeline_id: str, run_id: str, params: Optional[Dict] = None) -> None:
        pipeline_config = self._get_pipeline_config(pipeline_id)
        run = self.runs[run_id]
        
        try:
            # Initialize extractor and output handler
            extractor_class = self._get_extractor_class(pipeline_config.extractor_class)
            extractor = extractor_class(pipeline_config.api_config)
            
            # Create pipeline instance using factory
            pipeline = PipelineFactory.create_pipeline(pipeline_config.dict())
            
            # Extract and transform data
            logger.info(f"Starting data extraction for pipeline {pipeline_id}")
            raw_data = await extractor.extract(params)
            transformed_data = extractor.transform(raw_data) if hasattr(extractor, 'transform') else raw_data
            
            # Write to outputs
            for output in pipeline.outputs:
                await output.write(transformed_data)
                await output.close()
            
            # Update run status
            run.status = "COMPLETED"
            run.end_time = datetime.now(UTC)
            run.records_processed = len(transformed_data)
            
        except Exception as e:
            error_msg = f"Pipeline execution failed: {str(e)}"
            logger.error(error_msg)
            run.status = "FAILED"
            run.end_time = datetime.now(UTC)
            run.errors.append(error_msg)
    
    async def list_pipeline_runs(
        self,
        pipeline_id: str,
        limit: int = 10,
        status: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        runs = [
            run for run in self.runs.values()
            if run.pipeline_id == pipeline_id
            and (not status or run.status == status)
            and (not start_time or run.start_time >= start_time)
            and (not end_time or run.start_time <= end_time)
        ]
        return [run.dict() for run in sorted(runs, key=lambda x: x.start_time, reverse=True)[:limit]]

    async def trigger_pipeline(self, pipeline_id: str, params: Optional[Dict] = None) -> Dict[str, str]:
        run_id = self._create_pipeline_run(pipeline_id)
        
        # Execute pipeline asynchronously
        import asyncio
        asyncio.create_task(self.execute_pipeline(pipeline_id, run_id, params))
        
        return {"run_id": run_id, "status": "triggered"}
    
    def get_run_status(self, pipeline_id: str, run_id: str) -> Dict[str, Any]:
        if run_id not in self.runs:
            raise KeyError(f"Run {run_id} not found")
            
        run = self.runs[run_id]
        return {
            "run_id": run.run_id,
            "pipeline_id": run.pipeline_id,
            "status": run.status,
            "start_time": run.start_time.isoformat(),
            "end_time": run.end_time.isoformat() if run.end_time else None,
            "records_processed": run.records_processed,
            "errors": run.errors
        } 