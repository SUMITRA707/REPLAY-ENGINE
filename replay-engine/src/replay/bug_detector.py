from typing import List, Dict
from datetime import datetime
import yaml
from dataclasses import dataclass
from ..adapters.redis_stream_adapter import StreamMessage
from ..common.logging_config import ReplayLogger
from ..common.metrics import MetricsCollector

@dataclass
class DetectedBug:
    bug_id: str
    bug_type: str
    severity: str
    event_id: str
    context: Dict

class BugDetector:
    def __init__(self):
        with open("configs/replay_config.yml", "r") as f:
            config = yaml.safe_load(f)
        self.config = config.get("bug_detection", {})
        self.error_levels = self.config.get("error_levels", ["ERROR", "FATAL", "CRITICAL"])
        self.gap_threshold_seconds = self.config.get("gap_threshold_seconds", 300)
        self.correlation_timeout_hours = self.config.get("correlation_timeout_hours", 1)
        self.last_event_times: Dict[str, datetime] = {}
        self.error_counts: Dict[str, int] = {}
        self.logger = ReplayLogger(__name__)
        self.metrics = MetricsCollector()

    async def analyze_event(self, event: StreamMessage, session_stats: Dict) -> List[DetectedBug]:
        """
        Analyze an event for potential bugs.

        Args:
            event: StreamMessage to analyze.
            session_stats: Session statistics including replay_id.

        Returns:
            List of detected bugs.
        """
        bugs = []
        fields = event.fields
        replay_id = session_stats.get("replay_id", "unknown")
        self.logger.set_replay_id(replay_id)
        self.metrics.replay_id = replay_id

        # Parse timestamp
        try:
            current_time = datetime.fromisoformat(fields["timestamp"].replace("Z", "+00:00"))
        except (KeyError, ValueError):
            self.logger.warning(f"Invalid timestamp in event {event.event_id}")
            return bugs

        # Error level detection
        if fields.get("level") in self.error_levels:
            bug_id = f"bug-{event.event_id}-error"
            bugs.append(DetectedBug(
                bug_id=bug_id,
                bug_type="error_event",
                severity="high",
                event_id=event.event_id,
                context={"message": fields.get("payload", {}), "level": fields.get("level")}
            ))
            self.metrics.record_bug_detected("error_event", "high")
            self.logger.info(f"Detected error event {bug_id}")

        # Timing gap detection
        last_time = self.last_event_times.get(fields.get("session_id", "default"))
        if last_time and (current_time - last_time).total_seconds() > self.gap_threshold_seconds:
            bug_id = f"bug-{event.event_id}-gap"
            bugs.append(DetectedBug(
                bug_id=bug_id,
                bug_type="timing_gap",
                severity="medium",
                event_id=event.event_id,
                context={"gap_seconds": (current_time - last_time).total_seconds()}
            ))
            self.metrics.record_bug_detected("timing_gap", "medium")
            self.logger.info(f"Detected timing gap {bug_id}")
        self.last_event_times[fields.get("session_id", "default")] = current_time

        # Repeated error detection
        error_key = f"{fields.get('source', 'unknown')}:{fields.get('level', 'unknown')}"
        self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
        if self.error_counts[error_key] > 3:  # Threshold for repeated errors
            bug_id = f"bug-{event.event_id}-repeated"
            bugs.append(DetectedBug(
                bug_id=bug_id,
                bug_type="repeated_error",
                severity="high",
                event_id=event.event_id,
                context={"error_count": self.error_counts[error_key], "source": fields.get("source")}
            ))
            self.metrics.record_bug_detected("repeated_error", "high")
            self.logger.info(f"Detected repeated error {bug_id}")

        return bugs