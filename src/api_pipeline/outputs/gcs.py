import io
import json
import csv
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Set

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv_arrow
import fastavro
from google.cloud import storage
from google.api_core import retry
from loguru import logger
from pydantic import BaseModel, Field, field_validator, model_validator

from api_pipeline.core.base import BaseOutput, OutputConfig


class JsonSchemaConstraints(BaseModel):
    """JSON Schema validation constraints."""
    # String constraints
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    format: Optional[str] = None  # email, date-time, uri, etc.
    
    # Number constraints
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    exclusive_minimum: Optional[float] = None
    exclusive_maximum: Optional[float] = None
    multiple_of: Optional[float] = None
    
    # Array constraints
    min_items: Optional[int] = None
    max_items: Optional[int] = None
    unique_items: Optional[bool] = None
    
    # Object constraints
    min_properties: Optional[int] = None
    max_properties: Optional[int] = None
    required: Optional[List[str]] = None
    
    # Common constraints
    enum: Optional[List[Any]] = None
    const: Optional[Any] = None

    @field_validator('pattern')
    @classmethod
    def validate_pattern(cls, v):
        """Ensure pattern is a valid regex."""
        if v:
            try:
                re.compile(v)
            except re.error:
                raise ValueError(f"Invalid regex pattern: {v}")
        return v


class GCSJsonField(BaseModel):
    """JSON field schema with support for nested structures and JSON Schema keywords."""
    name: str
    type: Union[str, List[str]]  # Support union types like ["string", "null"]
    fields: Optional[List['GCSJsonField']] = None  # For nested objects
    items: Optional[Union['GCSJsonField', List['GCSJsonField']]] = None  # For arrays
    description: Optional[str] = None
    constraints: Optional[JsonSchemaConstraints] = None
    ref: Optional[str] = None  # Reference to a definition
    definitions: Optional[Dict[str, 'GCSJsonField']] = None  # Reusable definitions
    additional_properties: Optional[Union[bool, 'GCSJsonField']] = None  # For objects

    @model_validator(mode='after')
    def validate_structure(self) -> 'GCSJsonField':
        """Validate schema structure and references."""
        field_type = self.type
        fields = self.fields
        items = self.items
        ref = self.ref
        definitions = self.definitions or {}

        # Handle type unions
        if isinstance(field_type, list):
            if not field_type:
                raise ValueError("Type list cannot be empty")
            if len(set(field_type)) != len(field_type):
                raise ValueError("Duplicate types in type union")

        # Validate based on type
        if not ref:  # Skip validation if using $ref
            base_type = field_type[0] if isinstance(field_type, list) else field_type
            base_type = base_type.lower()

            if base_type == 'object' and not fields and not self.additional_properties:
                raise ValueError("Object type must have 'fields' defined or allow additional properties")
            if base_type == 'array' and not items:
                raise ValueError("Array type must have 'items' defined")

        # Validate references
        if ref:
            if not ref.startswith('#/definitions/'):
                raise ValueError("References must start with '#/definitions/'")
            ref_name = ref.split('/')[-1]
            if not definitions or ref_name not in definitions:
                raise ValueError(f"Referenced definition '{ref_name}' not found")

        return self


class GCSFieldSchema(BaseModel):
    """Schema definition for structured formats."""
    name: str
    type: str
    mode: str = "NULLABLE"
    description: Optional[str] = None
    json_schema: Optional[GCSJsonField] = None  # For JSON format


class GCSConfig(BaseModel):
    """Configuration for GCS output."""
    bucket: str
    prefix: str
    file_format: str = "jsonl"
    schema: Optional[List[GCSFieldSchema]] = None
    partition_by: Optional[Union[str, List[str]]] = None
    compression: Optional[str] = None
    batch_size: int = 1000
    filename_template: str = "{prefix}/{partitions}/data_{timestamp}_{batch}.{ext}"
    csv_config: Optional[Dict[str, Any]] = None
    validate_json: bool = True
    json_definitions: Optional[Dict[str, GCSJsonField]] = None  # Global JSON schema definitions


class GCSOutput(BaseOutput):
    """Handler for writing data to Google Cloud Storage."""

    SUPPORTED_FORMATS = {"jsonl", "csv", "parquet", "avro"}
    COMPRESSION_MAPPING = {
        "parquet": {"gzip": "gzip", "snappy": "snappy"},
        "csv": {"gzip": "gzip", "bzip2": "bz2"},
        "avro": {"snappy": "snappy", "deflate": "deflate"}
    }

    def __init__(self, config: OutputConfig):
        super().__init__(config)
        self.gcs_config = GCSConfig(**config.config)
        self._setup_json_definitions()
        
        if self.gcs_config.file_format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.gcs_config.file_format}")
        
        # Validate schema requirements
        if self.gcs_config.file_format in {"parquet", "avro"} and not self.gcs_config.schema:
            raise ValueError(f"Schema is required for {self.gcs_config.file_format} format")
        
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.gcs_config.bucket)
        self._ensure_bucket_exists()
        
        # Initialize batch counter
        self.current_batch = 0
        self.current_batch_data: List[Dict[str, Any]] = []

    def _setup_json_definitions(self) -> None:
        """Set up JSON schema definitions for reuse."""
        self.json_definitions = self.gcs_config.json_definitions or {}
        # Validate all references in definitions
        self._validate_json_definitions()

    def _validate_json_definitions(self) -> None:
        """Validate that all references in definitions are valid."""
        def collect_refs(schema: GCSJsonField) -> Set[str]:
            refs = set()
            if schema.ref:
                refs.add(schema.ref.split('/')[-1])
            if schema.fields:
                for field in schema.fields:
                    refs.update(collect_refs(field))
            if schema.items:
                if isinstance(schema.items, list):
                    for item in schema.items:
                        refs.update(collect_refs(item))
                else:
                    refs.update(collect_refs(schema.items))
            return refs

        # Collect all references
        all_refs = set()
        for definition in self.json_definitions.values():
            all_refs.update(collect_refs(definition))

        # Validate references
        for ref in all_refs:
            if ref not in self.json_definitions:
                raise ValueError(f"Referenced definition '{ref}' not found")

    def _resolve_json_schema(self, schema: GCSJsonField) -> GCSJsonField:
        """Resolve JSON schema references."""
        if schema.ref:
            ref_name = schema.ref.split('/')[-1]
            base_schema = self.json_definitions[ref_name]
            # Merge with original schema, allowing overrides
            return GCSJsonField(
                **{**base_schema.dict(exclude={'name'}), **schema.dict(exclude_unset=True)}
            )
        return schema

    def _validate_json_constraints(self, value: Any, constraints: JsonSchemaConstraints) -> bool:
        """Validate value against JSON Schema constraints."""
        try:
            if constraints.enum is not None and value not in constraints.enum:
                return False
            
            if constraints.const is not None and value != constraints.const:
                return False

            if isinstance(value, str):
                if constraints.min_length is not None and len(value) < constraints.min_length:
                    return False
                if constraints.max_length is not None and len(value) > constraints.max_length:
                    return False
                if constraints.pattern and not re.match(constraints.pattern, value):
                    return False
                if constraints.format:
                    # Add format validation (email, date-time, etc.)
                    pass

            if isinstance(value, (int, float)):
                if constraints.minimum is not None and value < constraints.minimum:
                    return False
                if constraints.maximum is not None and value > constraints.maximum:
                    return False
                if constraints.exclusive_minimum is not None and value <= constraints.exclusive_minimum:
                    return False
                if constraints.exclusive_maximum is not None and value >= constraints.exclusive_maximum:
                    return False
                if constraints.multiple_of is not None and value % constraints.multiple_of != 0:
                    return False

            if isinstance(value, list):
                if constraints.min_items is not None and len(value) < constraints.min_items:
                    return False
                if constraints.max_items is not None and len(value) > constraints.max_items:
                    return False
                if constraints.unique_items and len(value) != len(set(map(str, value))):
                    return False

            if isinstance(value, dict):
                if constraints.min_properties is not None and len(value) < constraints.min_properties:
                    return False
                if constraints.max_properties is not None and len(value) > constraints.max_properties:
                    return False
                if constraints.required:
                    if not all(req in value for req in constraints.required):
                        return False

            return True
        except Exception as e:
            logger.error(f"Constraint validation error: {str(e)}")
            return False

    def _validate_json_data(self, data: Any, schema: GCSJsonField) -> bool:
        """Recursively validate JSON data against schema with constraints."""
        try:
            # Resolve references
            schema = self._resolve_json_schema(schema)
            
            # Handle null values
            if data is None:
                return (isinstance(schema.type, list) and "null" in schema.type) or schema.type == "null"
            
            # Validate constraints if present
            if schema.constraints and not self._validate_json_constraints(data, schema.constraints):
                return False

            # Validate type
            types = [schema.type] if isinstance(schema.type, str) else schema.type
            valid_type = False
            for t in types:
                t = t.lower()
                if (
                    (t == "string" and isinstance(data, str)) or
                    (t == "number" and isinstance(data, (int, float))) or
                    (t == "integer" and isinstance(data, int)) or
                    (t == "boolean" and isinstance(data, bool)) or
                    (t == "null" and data is None)
                ):
                    valid_type = True
                    break
                elif t == "object" and isinstance(data, dict):
                    valid_type = True
                    # Validate object fields
                    if schema.fields:
                        for field in schema.fields:
                            if field.name not in data and field.name not in (schema.constraints.required or []):
                                continue
                            if field.name in data and not self._validate_json_data(data[field.name], field):
                                return False
                    # Validate additional properties
                    if schema.additional_properties is not False:
                        extra_fields = set(data.keys()) - {f.name for f in (schema.fields or [])}
                        if extra_fields and isinstance(schema.additional_properties, GCSJsonField):
                            for field in extra_fields:
                                if not self._validate_json_data(data[field], schema.additional_properties):
                                    return False
                    break
                elif t == "array" and isinstance(data, list):
                    valid_type = True
                    # Validate array items
                    if isinstance(schema.items, list):
                        # Tuple validation
                        if len(data) != len(schema.items):
                            return False
                        return all(self._validate_json_data(item, item_schema)
                                 for item, item_schema in zip(data, schema.items))
                    else:
                        # List validation
                        return all(self._validate_json_data(item, schema.items)
                                 for item in data)
            
            return valid_type
        except Exception as e:
            logger.error(f"JSON validation error: {str(e)}")
            return False

    def _ensure_bucket_exists(self) -> None:
        """Ensure the bucket exists."""
        if not self.bucket.exists():
            logger.warning(f"Bucket {self.gcs_config.bucket} does not exist")
            raise ValueError(f"Bucket {self.gcs_config.bucket} does not exist")

    def _get_compression(self) -> Optional[str]:
        """Get format-specific compression type."""
        if not self.gcs_config.compression:
            return None
        
        format_compressions = self.COMPRESSION_MAPPING.get(self.gcs_config.file_format, {})
        return format_compressions.get(self.gcs_config.compression)

    def _convert_to_arrow_schema(self) -> pa.Schema:
        """Convert GCS schema to Arrow schema for Parquet."""
        fields = []
        type_mapping = {
            "STRING": pa.string(),
            "INTEGER": pa.int64(),
            "FLOAT": pa.float64(),
            "BOOLEAN": pa.bool_(),
            "TIMESTAMP": pa.timestamp('us'),
            "DATE": pa.date32(),
            "BYTES": pa.binary()
        }

        for field in self.gcs_config.schema:
            arrow_type = type_mapping.get(field.type.upper())
            if not arrow_type:
                raise ValueError(f"Unsupported type for Parquet: {field.type}")
            
            if field.mode == "REPEATED":
                arrow_type = pa.list_(arrow_type)
            
            fields.append(pa.field(
                field.name,
                arrow_type,
                field.mode == "NULLABLE",
                field.description
            ))
        
        return pa.schema(fields)

    def _convert_to_avro_schema(self) -> Dict:
        """Convert GCS schema to AVRO schema."""
        type_mapping = {
            "STRING": "string",
            "INTEGER": "long",
            "FLOAT": "double",
            "BOOLEAN": "boolean",
            "TIMESTAMP": {"type": "long", "logicalType": "timestamp-micros"},
            "DATE": {"type": "int", "logicalType": "date"},
            "BYTES": "bytes"
        }

        fields = []
        for field in self.gcs_config.schema:
            avro_type = type_mapping.get(field.type.upper())
            if not avro_type:
                raise ValueError(f"Unsupported type for AVRO: {field.type}")
            
            field_schema = {
                "name": field.name,
                "type": ["null", avro_type] if field.mode == "NULLABLE" else avro_type
            }
            
            if field.description:
                field_schema["doc"] = field.description
            
            fields.append(field_schema)

        return {
            "type": "record",
            "name": "Record",
            "fields": fields
        }

    def _get_file_extension(self) -> str:
        """Get file extension based on format and compression."""
        ext = self.gcs_config.file_format
        if self.gcs_config.compression:
            ext = f"{ext}.{self.gcs_config.compression}"
        return ext

    async def _write_jsonl(self, blob: storage.Blob, data: List[Dict[str, Any]]) -> None:
        """Write data in JSONL format with optional schema validation."""
        if self.gcs_config.validate_json and self.gcs_config.schema:
            # Validate each record against the JSON schema
            for record in data:
                for field in self.gcs_config.schema:
                    if field.json_schema and field.name in record:
                        if not self._validate_json_data(record[field.name], field.json_schema):
                            raise ValueError(
                                f"Record validation failed for field {field.name}: {record[field.name]}"
                            )

        content = "\n".join(json.dumps(record) for record in data)
        blob.upload_from_string(
            content,
            content_type="application/json",
            retry=retry.Retry(predicate=retry.if_transient_error)
        )

    async def _write_csv(self, blob: storage.Blob, data: List[Dict[str, Any]]) -> None:
        """Write data in CSV format."""
        output = io.StringIO()
        if not data:
            return

        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames,
            **(self.gcs_config.csv_config or {})
        )
        
        writer.writeheader()
        writer.writerows(data)
        
        blob.upload_from_string(
            output.getvalue(),
            content_type="text/csv",
            retry=retry.Retry(predicate=retry.if_transient_error)
        )

    async def _write_parquet(self, blob: storage.Blob, data: List[Dict[str, Any]]) -> None:
        """Write data in Parquet format."""
        if not data:
            return

        # Convert to Arrow table
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df, schema=self._convert_to_arrow_schema())
        
        # Write to buffer
        output = io.BytesIO()
        pq.write_table(
            table,
            output,
            compression=self._get_compression()
        )
        
        blob.upload_from_string(
            output.getvalue(),
            content_type="application/parquet",
            retry=retry.Retry(predicate=retry.if_transient_error)
        )

    async def _write_avro(self, blob: storage.Blob, data: List[Dict[str, Any]]) -> None:
        """Write data in AVRO format."""
        if not data:
            return

        output = io.BytesIO()
        schema = self._convert_to_avro_schema()
        
        fastavro.writer(
            output,
            schema,
            data,
            codec=self._get_compression()
        )
        
        blob.upload_from_string(
            output.getvalue(),
            content_type="application/avro",
            retry=retry.Retry(predicate=retry.if_transient_error)
        )

    async def _write_batch(self, partition_path: str) -> None:
        """Write a batch of data to GCS."""
        if not self.current_batch_data:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.gcs_config.filename_template.format(
            prefix=self.gcs_config.prefix.rstrip("/"),
            partitions=partition_path,
            timestamp=timestamp,
            batch=self.current_batch,
            ext=self._get_file_extension()
        )

        blob = self.bucket.blob(filename)
        
        # Call appropriate writer based on format
        format_writers = {
            "jsonl": self._write_jsonl,
            "csv": self._write_csv,
            "parquet": self._write_parquet,
            "avro": self._write_avro
        }
        
        writer = format_writers.get(self.gcs_config.file_format)
        await writer(blob, self.current_batch_data)
        
        logger.info(
            f"Wrote {len(self.current_batch_data)} records to "
            f"gs://{self.gcs_config.bucket}/{filename}"
        )
        
        # Clear batch
        self.current_batch_data = []
        self.current_batch += 1

    @retry.Retry(predicate=retry.if_transient_error)
    async def write(self, data: List[Dict[str, Any]]) -> None:
        """Write data to GCS with partitioning and batching."""
        if not data:
            logger.warning("No data to write to GCS")
            return

        # Group data by partition path if partitioning is enabled
        partitioned_data: Dict[str, List[Dict[str, Any]]] = {}
        for record in data:
            partition_path = self._get_partition_path(record)
            if partition_path not in partitioned_data:
                partitioned_data[partition_path] = []
            partitioned_data[partition_path].append(record)

        # Write each partition
        for partition_path, partition_data in partitioned_data.items():
            for record in partition_data:
                self.current_batch_data.append(record)
                
                if len(self.current_batch_data) >= self.gcs_config.batch_size:
                    await self._write_batch(partition_path)

            # Write any remaining records in the batch
            if self.current_batch_data:
                await self._write_batch(partition_path)

    async def close(self) -> None:
        """Ensure any remaining data is written and close the client."""
        # Write any remaining data in the current batch
        if self.current_batch_data:
            partition_path = self._get_partition_path(self.current_batch_data[0])
            await self._write_batch(partition_path)
        
        # Close the client
        self.client.close() 