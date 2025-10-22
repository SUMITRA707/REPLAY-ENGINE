"""
Metrics collection for the replay engine using Prometheus
"""

import time
from typing import Dict, Any
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, generate_latest
import logging

logger = logging.getLogger(__name__)

# Create a custom registry for replay engine metrics
REGISTRY = CollectorRegistry()

# Event processing metrics
EVENTS_PROCESSED_TOTAL = Counter(
    'replay_events_processed_total',
    'Total number of events processed',
    ['replay_id', 'status'],
    registry=REGISTRY
)

EVENTS_ERRORS_TOTAL = Counter(
    'replay_events_errors_total',
    'Total number of event processing errors',
    ['replay_id', 'error_type'],
    registry=REGISTRY
)

# Replay progress metrics
REPLAY_PROGRESS = Gauge(
    'replay_progress_ratio',
    'Current replay progress as a ratio (0.0 to 1.0)',
    ['replay_id'],
    registry=REGISTRY
)

REPLAY_DURATION_SECONDS = Histogram(
    'replay_duration_seconds',
    'Duration of replay sessions in seconds',
    ['replay_id', 'status'],
    buckets=[1, 5, 10, 30, 60, 300, 600, 1800, 3600, float('inf')],
    registry=REGISTRY
)

# Redis connection metrics
REDIS_CONNECTIONS_ACTIVE = Gauge(
    'redis_connections_active',
    'Number of active Redis connections',
    registry=REGISTRY
)

REDIS_STREAM_LENGTH = Gauge(
    'redis_stream_length',
    'Current length of the Redis stream',
    ['stream_key'],
    registry=REGISTRY
)

# Checkpoint metrics
CHECKPOINT_OPERATIONS_TOTAL = Counter(
    'replay_checkpoint_operations_total',
    'Total number of checkpoint operations',
    ['operation_type', 'status'],
    registry=REGISTRY
)

# Bug detection metrics
BUGS_DETECTED_TOTAL = Counter(
    'replay_bugs_detected_total',
    'Total number of bugs detected',
    ['bug_type', 'severity'],
    registry=REGISTRY
)


class MetricsCollector:
    """Centralized metrics collection for replay operations"""
    
    def __init__(self, replay_id: str = None):
        self.replay_id = replay_id or "unknown"
        self.start_time = None
    
    def start_replay(self):
        """Mark the start of a replay session"""
        self.start_time = time.time()
        logger.info(f"Started replay metrics collection for {self.replay_id}")
    
    def end_replay(self, status: str = "completed"):
        """Mark the end of a replay session"""
        if self.start_time:
            duration = time.time() - self.start_time
            REPLAY_DURATION_SECONDS.labels(
                replay_id=self.replay_id,
                status=status
            ).observe(duration)
            logger.info(f"Replay {self.replay_id} completed in {duration:.2f}s")
    
    def record_event_processed(self, status: str = "success"):
        """Record a successfully processed event"""
        EVENTS_PROCESSED_TOTAL.labels(
            replay_id=self.replay_id,
            status=status
        ).inc()
    
    def record_event_error(self, error_type: str):
        """Record an event processing error"""
        EVENTS_ERRORS_TOTAL.labels(
            replay_id=self.replay_id,
            error_type=error_type
        ).inc()
    
    def update_progress(self, progress: float):
        """Update replay progress (0.0 to 1.0)"""
        REPLAY_PROGRESS.labels(replay_id=self.replay_id).set(progress)
    
    def record_checkpoint(self, operation_type: str, status: str = "success"):
        """Record a checkpoint operation"""
        CHECKPOINT_OPERATIONS_TOTAL.labels(
            operation_type=operation_type,
            status=status
        ).inc()
    
    def record_bug_detected(self, bug_type: str, severity: str = "medium"):
        """Record a detected bug"""
        BUGS_DETECTED_TOTAL.labels(
            bug_type=bug_type,
            severity=severity
        ).inc()
    
    def update_redis_stream_length(self, stream_key: str, length: int):
        """Update Redis stream length metric"""
        REDIS_STREAM_LENGTH.labels(stream_key=stream_key).set(length)
    
    def update_redis_connections(self, count: int):
        """Update active Redis connections count"""
        REDIS_CONNECTIONS_ACTIVE.set(count)


def get_metrics() -> bytes:
    """Get current metrics in Prometheus format"""
    return generate_latest(REGISTRY)


def get_metrics_summary() -> Dict[str, Any]:
    """Get a summary of current metrics"""
    # This is a simplified version - in production you'd want more sophisticated aggregation
    return {
        "registry_size": len(REGISTRY._names_to_collectors),
        "timestamp": time.time()
    }