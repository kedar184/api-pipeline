from abc import ABC, abstractmethod
from datetime import datetime, UTC
from typing import Any, Dict, Optional, Set
import time
from google.cloud import monitoring_v3
from loguru import logger
import asyncio



class BaseMetricsHandler(ABC):
    """Base class for metrics handling."""

    def __init__(self, metric_type: str):
        """Initialize the metrics handler.
        
        Args:
            metric_type: Type of metrics being handled (e.g., "extractor", "output")
        """
        self.metric_type = metric_type
        self._start_time = time.monotonic()
        self.pipeline_id: Optional[str] = None
        self.run_id: Optional[str] = None
        self._metrics: Dict[str, Any] = {}
        self._cloud_metrics: Optional['CloudMetricsHandler'] = None
        self._cloud_labels: Dict[str, str] = {}
        self._initialize_metrics()

    def set_run_context(self, pipeline_id: str, run_id: str) -> None:
        """Set the pipeline and run context for metrics."""
        self.pipeline_id = pipeline_id
        self.run_id = run_id

    def set_cloud_metrics(self, cloud_metrics: 'CloudMetricsHandler', labels: Dict[str, str]) -> None:
        """Set cloud metrics handler and labels.
        
        Args:
            cloud_metrics: Handler for cloud monitoring
            labels: Base labels to attach to all metrics
        """
        self._cloud_metrics = cloud_metrics
        self._cloud_labels = labels or {}

    @abstractmethod
    def _initialize_metrics(self) -> None:
        """Initialize metric values. Must be implemented by subclasses."""
        pass

    def _get_base_metrics(self) -> Dict[str, Any]:
        """Get base metrics common to all handlers."""
        current_time = time.monotonic()
        base_metrics = {
            'uptime': current_time - self._start_time,
            'metric_type': self.metric_type,
            'timestamp': datetime.now(UTC).isoformat()
        }
        if self.pipeline_id:
            base_metrics['pipeline_id'] = self.pipeline_id
        if self.run_id:
            base_metrics['run_id'] = self.run_id
        return base_metrics

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics with base metrics included."""
        metrics = self._get_base_metrics()
        metrics.update(self._metrics)
        return metrics

    async def update_metric(self, name: str, value: Any) -> None:
        """Update a metric value and optionally write to cloud monitoring.
        
        Args:
            name: Name of the metric
            value: Value to set (must be numeric or string)
            
        Raises:
            ValueError: If value type is not supported
        """
        if not isinstance(value, (int, float, str, bool)):
            raise ValueError(f"Unsupported metric value type: {type(value)}")

        try:
            # Update local metrics
            self._metrics[name] = value
            
            # Update cloud metrics if enabled
            if self._cloud_metrics:
                try:
                    labels = {
                        **self._cloud_labels,
                        "pipeline_id": self.pipeline_id or "unknown",
                        "run_id": self.run_id or "unknown"
                    }
                    await self._cloud_metrics.write_metrics_batch_async({name: value}, labels)
                except Exception as e:
                    logger.warning(f"Failed to write metric {name} to cloud monitoring: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to update metric {name}: {str(e)}")
            raise


class ExtractorMetricsHandler(BaseMetricsHandler):
    """Metrics handler for data extractors."""

    def __init__(self):
        super().__init__(metric_type="extractor")

    def _initialize_metrics(self) -> None:
        """Initialize extractor-specific metrics."""
        self._metrics.update({
            'requests_made': 0,
            'requests_failed': 0,
            'items_processed': 0,
            'retry_attempts': 0,
            'total_processing_time': 0.0,
            'rate_limit_hits': 0,
            'auth_failures': 0,
            'watermark_updates': 0,
            'window_count': 0,
            'last_request_time': None
        })

    def get_metrics(self) -> Dict[str, Any]:
        """Get current run metrics for the extractor."""
        metrics = super().get_metrics()
        # Calculate derived metrics
        uptime = metrics['uptime']
        requests_made = metrics['requests_made']
        
        metrics['requests_per_second'] = (
            requests_made / uptime if uptime > 0 else 0
        )
        metrics['success_rate'] = (
            (requests_made - metrics['requests_failed']) / requests_made
            if requests_made > 0 else 0
        )
        return metrics


class OutputMetricsHandler(BaseMetricsHandler):
    """Metrics handler for data outputs."""

    def __init__(self):
        super().__init__(metric_type="output")

    def _initialize_metrics(self) -> None:
        """Initialize output-specific metrics."""
        self._metrics.update({
            'records_written': 0,
            'bytes_written': 0,
            'write_errors': 0,
            'last_write_time': None,
            'total_write_time': 0.0,
            'batch_count': 0
        })

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics for the output."""
        metrics = super().get_metrics()
        # Calculate derived metrics
        uptime = metrics['uptime']
        records_written = metrics['records_written']
        
        metrics['records_per_second'] = (
            records_written / uptime if uptime > 0 else 0
        )
        metrics['average_write_time'] = (
            metrics['total_write_time'] / records_written
            if records_written > 0 else 0
        )
        metrics['error_rate'] = (
            metrics['write_errors'] / metrics['batch_count']
            if metrics['batch_count'] > 0 else 0
        )
        return metrics


class CloudMetricsHandler:
    """Handler for writing metrics to Google Cloud Monitoring."""

    def __init__(self, project_id: str, metric_prefix: str = "api_pipeline"):
        """Initialize the cloud metrics handler.
        
        Args:
            project_id: Google Cloud project ID
            metric_prefix: Prefix for custom metrics
        """
        self.project_id = project_id
        self.metric_prefix = metric_prefix
        self.client = monitoring_v3.MetricServiceClient()
        self._pending_tasks: Set[asyncio.Task] = set()

    async def write_metrics_batch_async(self, metrics: Dict[str, Any], labels: Dict[str, str]) -> None:
        """Write a batch of metrics asynchronously.
        
        Creates a background task to write metrics without blocking the caller.
        Tracks pending tasks to ensure they complete during cleanup.
        
        Args:
            metrics: Dictionary of metric names and values
            labels: Labels to attach to all metrics
        """
        try:
            task = asyncio.create_task(
                self.write_metrics_batch(metrics, labels),
                name=f"metrics_batch_{int(time.time())}"
            )
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)
        except Exception as e:
            logger.warning(f"Failed to schedule metrics batch write: {str(e)}")

    async def write_metrics_batch(self, metrics: Dict[str, Any], labels: Dict[str, str]) -> None:
        """Write a batch of metrics to Cloud Monitoring.
        
        Args:
            metrics: Dictionary of metric names and values
            labels: Labels to attach to all metrics
        """
        try:
            tasks = [
                self.write_metric(name, value, labels)
                for name, value in metrics.items()
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Failed to write metrics batch: {str(e)}")

    async def write_metric(self, metric_name: str, value: Any, labels: Dict[str, str]) -> None:
        """Write a single metric to Cloud Monitoring.
        
        Args:
            metric_name: Name of the metric
            value: Metric value (must be numeric)
            labels: Labels to attach to the metric
            
        Raises:
            ValueError: If value cannot be converted to float
        """
        try:
            # Validate and convert value
            try:
                float_value = float(value)
            except (TypeError, ValueError):
                raise ValueError(f"Metric value must be numeric, got {type(value)}")

            series = monitoring_v3.TimeSeries()
            series.metric.type = f"custom.googleapis.com/{self.metric_prefix}/{metric_name}"
            series.metric.labels.update(labels)
            
            now = time.time()
            point = monitoring_v3.Point()
            point.value.double_value = float_value
            point.interval.end_time.seconds = int(now)
            series.points.append(point)
            
            self.client.create_time_series(
                request={
                    "name": f"projects/{self.project_id}",
                    "time_series": [series]
                }
            )
        except Exception as e:
            logger.error(f"Failed to write metric {metric_name}: {str(e)}")
            raise

    async def cleanup(self) -> None:
        """Wait for all pending metric writes to complete."""
        if self._pending_tasks:
            try:
                await asyncio.gather(*self._pending_tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"Error during metrics cleanup: {str(e)}")
            finally:
                self._pending_tasks.clear() 