from typing import Dict
from datetime import datetime
import asyncio
import yaml
import os
import json
from ..adapters.redis_stream_adapter import RedisStreamAdapter
from ..replay.checkpoint_store import CheckpointStore
from ..replay.session_manager import SessionManager
from ..replay.bug_detector import BugDetector
from ..common.metrics import MetricsCollector
from ..common.logging_config import ReplayLogger

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
        # Sanitize config values
        config.setdefault("mode", "dry-run")
        config.setdefault("speed", 1.0)
        config["end_ts"] = config.get("end_ts") or "+"
        config["start_ts"] = config.get("start_ts") or "0"

        replay_id = config.get("replay_id", "unknown")
        
        try:
            self.logger.set_replay_id(replay_id)
            self.metrics.replay_id = replay_id
            self.metrics.start_replay()

            # CRITICAL: Connect to Redis before any operations
            await self.redis_adapter.connect()
            self.logger.info(f"Connected to Redis for replay {replay_id}")

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
                end_id=config.get("end_ts") or "+",
                count=self.config["replay"]["max_events_per_batch"]
            )

            self.logger.info(f"Fetched {len(events)} events for replay {replay_id}")

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
            if events:
                await self.checkpoint_store.save_checkpoint(
                    replay_id,
                    {
                        "events_processed": events_processed,
                        "current_message_id": events[-1].stream_id,
                        "progress": 1.0,
                        "completed_at": datetime.utcnow().isoformat() + "Z"
                    }
                )
            
            # Generate reports BEFORE updating session status
            os.makedirs("/app/reports", exist_ok=True)
            
            # Generate JSON report
            report_data = {
                "replay_id": replay_id,
                "status": "completed",
                "events_processed": events_processed,
                "total_events": total_events,
                "progress": 1.0,
                "started_at": session.started_at.isoformat() if session.started_at else None,
                "completed_at": datetime.utcnow().isoformat() + "Z",
                "bugs_detected": self.metrics.bugs_detected if hasattr(self.metrics, 'bugs_detected') else [],
                "events": []
            }
            
            with open(f"/app/reports/replay_{replay_id}.json", "w") as f:
                json.dump(report_data, f, indent=2)
            
            # Generate HTML report
            html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Replay Report - {replay_id}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        h1 {{ color: #333; }}
        .metric {{ margin: 10px 0; }}
        .metric strong {{ color: #555; }}
    </style>
</head>
<body>
    <h1>Replay Report: {replay_id}</h1>
    <div class="metric"><strong>Status:</strong> {report_data['status']}</div>
    <div class="metric"><strong>Events Processed:</strong> {events_processed} / {total_events}</div>
    <div class="metric"><strong>Progress:</strong> {progress * 100:.1f}%</div>
    <div class="metric"><strong>Completed At:</strong> {report_data['completed_at']}</div>
    <div class="metric"><strong>Started At:</strong> {report_data['started_at']}</div>
</body>
</html>"""
            
            with open(f"/app/reports/replay_{replay_id}.html", "w") as f:
                f.write(html_content)
            
            self.logger.info(f"Generated reports for replay {replay_id}")
            
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
        
        finally:
            # CRITICAL: Always disconnect Redis
            try:
                await self.redis_adapter.disconnect()
                self.logger.info(f"Disconnected Redis for replay {replay_id}")
            except Exception as e:
                self.logger.warning(f"Failed to disconnect Redis: {e}")