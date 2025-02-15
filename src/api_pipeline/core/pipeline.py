from typing import Any, Dict, List, Optional
from datetime import datetime, UTC
import uuid

from loguru import logger
from pydantic import BaseModel

from api_pipeline.core.base import BaseExtractor, BaseOutput
from api_pipeline.core.models import PipelineRun, PipelineRunConfig


class PipelineRunConfig(BaseModel):
    """Configuration for a pipeline run."""
    pipeline_id: str
    parameters: Optional[Dict[str, Any]] = None
    output_config: Optional[Dict[str, Any]] = None


class PipelineRun(BaseModel):
    """Represents a single pipeline run."""
    run_id: str
    pipeline_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    parameters: Dict[str, Any]
    records_processed: int = 0
    errors: List[str] = []


class Pipeline:
    """Core pipeline implementation."""

    def __init__(
        self,
        pipeline_id: str,
        extractor: BaseExtractor,
        outputs: List[BaseOutput]
    ):
        self.pipeline_id = pipeline_id
        self.extractor = extractor
        self.outputs = outputs

    async def execute(self, config: PipelineRunConfig) -> PipelineRun:
        """Execute the pipeline and return run details."""
        run_id = str(uuid.uuid4())
        run = PipelineRun(
            run_id=run_id,
            pipeline_id=self.pipeline_id,
            status="running",
            start_time=datetime.now(UTC),
            parameters=config.parameters or {}
        )

        try:
            # Extract data
            logger.info(f"Starting data extraction for pipeline {self.pipeline_id}")
            data = await self.extractor.extract(config.parameters)
            logger.info(f"Extracted {len(data)} records")
            
            # Write to all outputs
            for output in self.outputs:
                logger.info(f"Writing to output {output.__class__.__name__}")
                await output.write(data)
            
            run.status = "completed"
            run.records_processed = len(data)
            logger.info(f"Pipeline {self.pipeline_id} completed successfully")

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            run.status = "failed"
            run.errors.append(str(e))
            raise

        finally:
            run.end_time = datetime.now(UTC)
            # Close all outputs
            for output in self.outputs:
                try:
                    await output.close()
                except Exception as e:
                    logger.error(f"Failed to close output {output.__class__.__name__}: {str(e)}")

        return run 