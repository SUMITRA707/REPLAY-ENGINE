import json
import asyncio
from typing import Dict, Any
from datetime import datetime

from ..adapters.redis_stream_adapter import RedisStreamAdapter
from ..replay.checkpoint_store import CheckpointStore
from ..replay.session_manager import SessionManager
from ..common.logging_config import ReplayLogger

class DeterministicReplayer:
    def __init__(self, redis_adapter: RedisStreamAdapter, checkpoint_store: CheckpointStore, session_manager: SessionManager):
        self.redis_adapter = redis_adapter
        self.checkpoint_store = checkpoint_store
        self.session_manager = session_manager
        self.logger = ReplayLogger(__name__)

    async def execute_replay(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a deterministic replay.

        Args:
            config: Replay configuration (replay_id, mode, speed, etc.).

        Returns:
            Result dict with summary.
        """
        replay_id = config['replay_id']
        mode = config['mode']
        speed = config.get('speed', 1.0)
        checkpoint_every = config.get('checkpoint_every', 100)

        self.logger.info(f"Starting replay {replay_id} in {mode} mode at {speed}x speed")

        # Create session
        self.session_manager.create_session(replay_id, mode)

        events_processed = 0
        bugs_detected = 0
        start_time = datetime.now()
        progress = 0.0
        elapsed = 0.0  # CRITICAL FIX: Initialize elapsed at the start

        try:
            # Ensure Redis adapter is connected
            if self.redis_adapter.redis_client is None:
                self.logger.info("Connecting to Redis...")
                await self.redis_adapter.connect()
            
            # Read events from Redis stream
            stream_entries = await self.redis_adapter.read_events(
                start_ts=config.get('start_ts'),
                end_ts=config.get('end_ts'),
                count=1000
            )
            
            self.logger.info(f"🔍 DEBUG: read_events returned {len(stream_entries)} events")
            for i, entry in enumerate(stream_entries[:3]):
                self.logger.info(f"   Event {i}: {entry.get('method')} {entry.get('path')} - status {entry.get('status')}")

            total_events = len(stream_entries)
            if total_events == 0:
                self.logger.warning("No events to replay")
                self.session_manager.complete_session(replay_id)
                return {"success": False, "message": "No events found"}

            for i, entry in enumerate(stream_entries):
                # Parse and store raw JSON for details
                raw_event_json = json.dumps(entry)
                
                # FIXED: Add visible delays for dashboard updates
                if mode == "dry-run":
                    await asyncio.sleep(0.5 / speed)  # 0.5 seconds per event
                elif mode == "timed":
                    if i > 0 and 'timestamp' in entry and 'timestamp' in stream_entries[i-1]:
                        try:
                            current_ts = datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00'))
                            prev_ts = datetime.fromisoformat(stream_entries[i-1]['timestamp'].replace('Z', '+00:00'))
                            delay = (current_ts - prev_ts).total_seconds() / speed
                            if delay > 0:
                                await asyncio.sleep(min(delay, 2.0))
                            else:
                                await asyncio.sleep(0.5 / speed)
                        except:
                            await asyncio.sleep(0.5 / speed)
                    else:
                        await asyncio.sleep(0.5 / speed)
                else:  # full
                    await asyncio.sleep(1.0 / speed)

                # Detect bugs
                anomaly = self._detect_anomaly(entry)
                if anomaly:
                    bugs_detected += 1

                events_processed += 1
                progress = events_processed / total_events

                # Update progress with detailed event info
                await self.session_manager.update_progress(
                    replay_id, progress, events_processed, bugs_detected,
                    raw_event_json=raw_event_json,
                    status='running',
                    current_event_id=f"{entry.get('method', 'GET')} {entry.get('path', 'Unknown')}"
                )

                # Checkpoint
                if events_processed % checkpoint_every == 0:
                    await self.checkpoint_store.save_checkpoint(replay_id, progress, events_processed)

                self.logger.info(f"✅ Processed event {events_processed}/{total_events}: {entry.get('method')} {entry.get('path')}")

            # Complete
            self.session_manager.complete_session(replay_id)
            elapsed = (datetime.now() - start_time).total_seconds()  # Calculate elapsed at end

            self.logger.info(f"Replay {replay_id} completed: {events_processed} events, {bugs_detected} bugs, {elapsed:.2f}s")

            return {
                "success": True,
                "replay_id": replay_id,
                "events_processed": events_processed,
                "bugs_detected": bugs_detected,
                "elapsed_seconds": elapsed,
                "message": f"Replay completed successfully in {mode} mode"
            }

        except Exception as e:
            # CRITICAL FIX: Calculate elapsed even on error
            elapsed = (datetime.now() - start_time).total_seconds()
            
            # FIXED: Use print() instead of logger.error() to avoid exc_info conflict
            print(f"❌ ERROR: Replay {replay_id} failed: {str(e)}")
            import traceback
            traceback.print_exc()  # Print full traceback
            
            # Update session status
            try:
                await self.session_manager.update_progress(
                    replay_id, progress, events_processed, bugs_detected, 
                    status='error', message=str(e)
                )
            except Exception as update_error:
                print(f"❌ Failed to update session status: {update_error}")
            
            return {
                "success": False, 
                "error": str(e),
                "replay_id": replay_id,
                "events_processed": events_processed,
                "bugs_detected": bugs_detected,
                "elapsed_seconds": elapsed
            }

    def _detect_anomaly(self, event: Dict[str, Any]) -> bool:
        """Detect anomalies/bugs in replayed event"""
        status = event.get('status', 200)
        if isinstance(status, (int, str)):
            try:
                status_code = int(status)
                if status_code >= 400:
                    return True
            except:
                pass
        return False