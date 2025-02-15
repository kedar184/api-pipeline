"""Output handlers for pipeline data."""

from api_pipeline.outputs.bigquery import BigQueryOutput
from api_pipeline.outputs.gcs import GCSOutput
from api_pipeline.outputs.local_json import LocalJsonOutput

# Export output classes
__all__ = [
    "BigQueryOutput",
    "GCSOutput",
    "LocalJsonOutput"
] 