from typing import Dict, List
from datetime import datetime, timezone
import asyncio
import yaml
from ..adapters.redis_stream_adapter import RedisStreamAdapter, StreamMessage
from ..replay.checkpoint_store import CheckpointStore
from ..replay.session_manager import SessionManager, ReplaySession
from ..replay.bug_detector import BugDetector
from ..common.metrics import MetricsCollector
from ..common.logging_config import ReplayLogger
import logging

class DeterministicReplayer:
    def __init__(self, redis_adapter: RedisStreamAdapter, checkpoint_store: CheckpointStore, session_manager: SessionManager):
        self.redis_adapter = redis_adapter
        self.checkpoint_store = checkpoint_store
        self.session_manager = session_manager
        self.bug_detector = BugDetector()
        self.logger = ReplayLogger(__name__)
        with open("configs/replay_config.yml", "r") as f:
            self.config = yaml.safe_load(f)
        self.metrics = MetricsCollector()

    async def execute_replay(self, config: Dict) -> Dict:
        """
        Execute a replay session with deterministic event ordering.

        Args:
            config: Replay configuration including replay_id, session_id, start_ts, end_ts, mode, speed.

        Returns:
            Dict with replay status and metrics.
        """
        try:
            replay_id = config["replay_id"]
            self.logger.set_replay_id(replay_id)
            self.metrics.replay_id = replay_id
            self.metrics.start_replay()

            # Update session status
            session = await self.session_manager.get_session(replay_id)
            if not session:
                raise ValueError(f"Session {replay_id} not found")
            await self.session_manager.update_session_status(replay_id, "running")

            # Load checkpoint if exists
            checkpoint = await self.checkpoint_store.load_checkpoint(replay_id)
            start_id = checkpoint.get("current_message_id", "0") if checkpoint else "0"
            events_processed = checkpoint.get("events_processed", 0) if checkpoint else 0

            # Fetch events
            events = await self.redis_adapter.read_messages_by_range(
                start_id=start_id,
                end_id=config.get("end_ts", "+"),
                count=self.config["replay"]["max_events_per_batch"]
            )

            # Sort events deterministically: timestamp, then event_id
            events.sort(key=lambda e: (
                datetime.fromisoformat(e.fields["timestamp"].replace("Z", "+00:00")),
                e.fields["event_id"]
            ))

            total_events = len(events)
            await self.session_manager.update_session_progress(replay_id, total_events=total_events)

            # Process events
            for i, event in enumerate(events):
                # Apply replay mode
                if config["mode"] == "timed" and config["speed"] != 1.0:
                    await asyncio.sleep(1.0 / config["speed"])

                # Analyze for bugs
                bugs = await self.bug_detector.analyze_event(event, {"replay_id": replay_id})
                for bug in bugs:
                    self.metrics.record_bug_detected(bug.bug_type, bug.severity)

                # Update progress
                events_processed += 1
                progress = events_processed / total_events if total_events > 0 else 0.0
                await self.session_manager.update_session_progress(
                    replay_id,
                    events_processed=events_processed,
                    progress=progress
                )
                self.metrics.update_progress(progress)
                self.metrics.record_event_processed()

                # Checkpoint periodically
                if events_processed % self.config["replay"]["checkpoint_every"] == 0:
                    await self.checkpoint_store.save_checkpoint(
                        replay_id,
                        {
                            "events_processed": events_processed,
                            "current_message_id": event.stream_id,
                            "progress": progress
                        }
                    )
                    self.metrics.record_checkpoint("main")

                # Acknowledge event
                await self.redis_adapter.acknowledge_message(event.stream_id)

            # Final checkpoint
            await self.checkpoint_store.save_checkpoint(
                replay_id,
                {
                    "events_processed": events_processed,
                    "current_message_id": events[-1].stream_id if events else start_id,
                    "progress": 1.0,
                    "completed_at": datetime.utcnow().isoformat() + "Z"
                }
            )
            await self.session_manager.update_session_status(replay_id, "completed")
            self.metrics.end_replay("completed")

            self.logger.info(f"Replay {replay_id} completed: {events_processed} events processed")
            return {
                "status": "completed",
                "events_processed": events_processed,
                "total_events": total_events,
                "progress": 1.0
            }

        except Exception as e:
            self.logger.error(f"Replay {replay_id} failed: {str(e)}")
            self.metrics.record_event_error(str(type(e).__name__))
            await self.session_manager.update_session_status(replay_id, "failed")
            self.metrics.end_replay("failed")
            raise