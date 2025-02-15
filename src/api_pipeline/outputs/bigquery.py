from typing import Any, Dict, List, Optional
from google.cloud import bigquery
from google.api_core import retry
from loguru import logger
from pydantic import BaseModel

from api_pipeline.core.base import BaseOutput, OutputConfig


class BigQueryFieldSchema(BaseModel):
    """Schema for a BigQuery field."""
    name: str
    type: str
    mode: str = "NULLABLE"
    description: Optional[str] = None


class BigQueryConfig(BaseModel):
    """Configuration for BigQuery output."""
    project_id: str
    dataset_id: str
    table_id: str
    schema: List[Dict[str, str]]
    create_if_needed: bool = True
    write_disposition: str = "WRITE_APPEND"
    partition_field: Optional[str] = None
    cluster_fields: Optional[List[str]] = None


class BigQueryOutput(BaseOutput):
    """Handler for writing data to BigQuery."""

    def __init__(self, config: OutputConfig):
        super().__init__(config)
        self.bq_config = BigQueryConfig(**config.config)
        self.client = bigquery.Client(project=self.bq_config.project_id)
        self._ensure_dataset_exists()
        if self.bq_config.create_if_needed:
            self._ensure_table_exists()

    def _ensure_dataset_exists(self) -> None:
        """Ensure the dataset exists, create if needed."""
        dataset_ref = self.client.dataset(self.bq_config.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Make configurable if needed
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset {self.bq_config.dataset_id}")

    def _ensure_table_exists(self) -> None:
        """Ensure the table exists with the correct schema."""
        table_ref = f"{self.bq_config.project_id}.{self.bq_config.dataset_id}.{self.bq_config.table_id}"
        
        # Convert schema to BigQuery format
        schema = []
        for field in self.bq_config.schema:
            field_schema = bigquery.SchemaField(
                name=field["name"],
                field_type=field["type"],
                mode=field.get("mode", "NULLABLE"),
                description=field.get("description", None)
            )
            schema.append(field_schema)

        # Create table with partitioning and clustering if specified
        table = bigquery.Table(table_ref, schema=schema)
        
        if self.bq_config.partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=self.bq_config.partition_field
            )
        
        if self.bq_config.cluster_fields:
            table.clustering_fields = self.bq_config.cluster_fields

        self.client.create_table(table, exists_ok=True)
        logger.info(f"Ensured table {table_ref} exists with correct schema")

    @retry.Retry(predicate=retry.if_transient_error)
    async def write(self, data: List[Dict[str, Any]]) -> None:
        """Write data to BigQuery with retry logic."""
        if not data:
            logger.warning("No data to write to BigQuery")
            return

        table_ref = f"{self.bq_config.project_id}.{self.bq_config.dataset_id}.{self.bq_config.table_id}"
        
        # Convert to load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=self.bq_config.write_disposition,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        try:
            load_job = self.client.load_table_from_json(
                data,
                table_ref,
                job_config=job_config
            )
            load_job.result()  # Wait for job to complete
            
            logger.info(
                f"Successfully loaded {len(data)} rows to {table_ref}"
            )
        except Exception as e:
            logger.error(f"Error writing to BigQuery: {str(e)}")
            raise

    async def close(self) -> None:
        """Close the BigQuery client."""
        self.client.close() 