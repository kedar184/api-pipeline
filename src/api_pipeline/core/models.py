from datetime import datetime, UTC
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class PipelineParameter(BaseModel):
    name: str = Field(...)
    type: str = Field(...)
    required: bool = Field(False)
    description: Optional[str] = Field(None)
    default: Optional[Any] = Field(None)

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        allowed_types = {'string', 'integer', 'float', 'boolean', 'array', 'object'}
        if v.lower() not in allowed_types:
            raise ValueError(f"Type must be one of: {allowed_types}")
        return v.lower()


class PipelineConfig(BaseModel):
    pipeline_id: str = Field(...)
    description: str = Field(...)
    enabled: bool = Field(True)
    extractor_class: str = Field(...)
    api_config: Dict = Field(...)
    parameters: List[PipelineParameter] = Field(default_factory=list)
    output: List[Dict] = Field(...)

    @field_validator('pipeline_id')
    @classmethod
    def validate_pipeline_id(cls, v):
        if not v.replace('_', '').replace('-', '').isalnum():
            raise ValueError("pipeline_id must contain only letters, numbers, underscores, and hyphens")
        return v

    @field_validator('extractor_class')
    @classmethod
    def validate_extractor_class(cls, v):
        if not all(part.isidentifier() for part in v.split('.')):
            raise ValueError("extractor_class must be a valid Python import path")
        return v


class PipelineRunStatus(BaseModel):
    status: str = Field(...)
    message: Optional[str] = Field(None)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator('status')
    @classmethod
    def validate_status(cls, v):
        allowed_statuses = {'pending', 'running', 'completed', 'failed', 'cancelled'}
        if v.lower() not in allowed_statuses:
            raise ValueError(f"Status must be one of: {allowed_statuses}")
        return v.lower()


class PipelineRun(BaseModel):
    run_id: str = Field(...)
    pipeline_id: str = Field(...)
    status: str = Field(...)
    start_time: datetime = Field(...)
    end_time: Optional[datetime] = Field(None)
    parameters: Dict = Field(default_factory=dict)
    records_processed: int = Field(0)
    errors: List[str] = Field(default_factory=list)
    statuses: List[PipelineRunStatus] = Field(default_factory=list)

    @field_validator('status')
    @classmethod
    def validate_status(cls, v):
        allowed_statuses = {'pending', 'running', 'completed', 'failed', 'cancelled'}
        if v.lower() not in allowed_statuses:
            raise ValueError(f"Status must be one of: {allowed_statuses}")
        return v.lower()

    @field_validator('end_time')
    @classmethod
    def validate_end_time(cls, v, info):
        if v and 'start_time' in info.data and v < info.data['start_time']:
            raise ValueError("end_time must be after start_time")
        return v


class PipelineRunRequest(BaseModel):
    parameters: Optional[Dict[str, Any]] = Field(default=None)
    output_config: Optional[Dict[str, Any]] = Field(default=None)


class PipelineStatus(BaseModel):
    pipeline_id: str = Field(...)
    status: str = Field(...)
    last_run: str = Field(...)
    records_processed: int = Field(0)
    errors: List[str] = Field(default_factory=list)