from typing import Dict, Optional, List
from datetime import datetime, UTC
from fastapi import FastAPI, HTTPException, Request, Query
import yaml
import os
from loguru import logger

from api_pipeline.core.pipeline_manager import PipelineManager
from api_pipeline.core.models import PipelineRunStatus, PipelineRequest

# Load environment config
config_path = os.getenv('CONFIG_PATH', 'config/config.yaml')
try:
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    logger.warning(f"Config file not found at {config_path}, using defaults")
    config = {
        'service': {
            'name': 'API Pipeline Manager',
            'version': '1.0.0',
            'vpc_access_only': False
        }
    }

app = FastAPI(
    title=config['service']['name'],
    version=config['service']['version'],
    docs_url=None if config.get('security', {}).get('vpc_access_only') else "/docs"
)

# Initialize manager
pipeline_manager = PipelineManager()


@app.post("/v1/pipelines/execute")
async def execute_pipeline(request: PipelineRequest) -> Dict:
    """Execute a pipeline with the given parameters."""
    try:
        return await pipeline_manager.trigger_pipeline(
            pipeline_id=request.pipeline_id,
            params=request.parameters
        )
    except ValueError as e:
        logger.error(f"Invalid request: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/v1/pipelines/{pipeline_id}/runs/{run_id}", response_model=PipelineRunStatus)
async def get_pipeline_run_status(pipeline_id: str, run_id: str) -> PipelineRunStatus:
    """Get the status of a specific pipeline run."""
    try:
        status = pipeline_manager.get_run_status(pipeline_id, run_id)
        if not status:
            raise HTTPException(status_code=404, detail="Pipeline run not found")
        return PipelineRunStatus(**status)
    except ValueError as e:
        logger.error(f"Invalid request: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to get pipeline run status: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/v1/pipelines/{pipeline_id}/runs", response_model=List[PipelineRunStatus])
async def list_pipeline_runs(
    pipeline_id: str,
    limit: int = Query(default=10, le=100),
    status: Optional[str] = Query(None, description="Filter by status"),
    start_time: Optional[datetime] = Query(None, description="Filter by start time after"),
    end_time: Optional[datetime] = Query(None, description="Filter by start time before")
) -> List[PipelineRunStatus]:
    """List pipeline runs with optional filtering."""
    try:
        runs = await pipeline_manager.list_pipeline_runs(
            pipeline_id=pipeline_id,
            limit=limit,
            status=status,
            start_time=start_time,
            end_time=end_time
        )
        return [PipelineRunStatus(**run) for run in runs]
    except ValueError as e:
        logger.error(f"Invalid request: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to list pipeline runs: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/health")
async def health_check() -> Dict:
    """Health check endpoint required by Cloud Run."""
    return await pipeline_manager.check_health()


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Add security headers to all responses."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response 