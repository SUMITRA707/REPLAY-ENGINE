import json
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime
from dataclasses import dataclass, asdict, field

from ..common.logging_config import ReplayLogger  # CORRECT

@dataclass
class ReplaySession:
    replay_id: str
    status: str = "idle"
    start_time: datetime = None
    progress: float = 0.0
    events_processed: int = 0
    bugs_detected: int = 0
    raw_event_json: str = None  # Store full Redis event JSON
    current_event_id: str = None
    message: str = None
    current_event_details: Dict[str, Any] = field(default_factory=lambda: {
        'method': 'GET', 'path': 'Unknown', 'activity': 'N/A', 'status': 'N/A'
    })

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        # Convert datetime to string for JSON serialization
        if self.start_time:
            data['start_time'] = self.start_time.isoformat()
        return data

class SessionManager:
    def __init__(self):
        self.sessions: Dict[str, ReplaySession] = {}
        self.logger = ReplayLogger(__name__)

    def create_session(self, replay_id: str, mode: str = "dry-run") -> ReplaySession:
        """
        Create a new replay session.

        Args:
            replay_id: Unique replay ID.
            mode: Replay mode (dry-run, timed, full).

        Returns:
            New ReplaySession.
        """
        session = ReplaySession(
            replay_id=replay_id,
            status="running",
            start_time=datetime.now(),
            progress=0.0,
            events_processed=0,
            bugs_detected=0
        )
        self.sessions[replay_id] = session
        self.logger.info(f"Created session {replay_id} in {mode} mode")
        return session

    async def update_progress(self, replay_id: str, progress: float, events_processed: int, bugs_detected: int = 0, **kwargs):
        """
        Update session progress (ASYNC VERSION - FIXED).

        Args:
            replay_id: Replay ID.
            progress: Progress fraction (0.0-1.0).
            events_processed: Number of events processed.
            bugs_detected: Number of bugs detected.
            **kwargs: Additional updates (e.g., status, raw_event_json).
        """
        session = await self.get_session(replay_id)
        if session:
            session.progress = progress
            session.events_processed = events_processed
            session.bugs_detected = bugs_detected
            
            # Store raw event JSON if provided
            if 'raw_event_json' in kwargs:
                session.raw_event_json = kwargs['raw_event_json']
            if 'status' in kwargs:
                session.status = kwargs['status']
            if 'current_event_id' in kwargs:
                session.current_event_id = kwargs['current_event_id']
            if 'message' in kwargs:
                session.message = kwargs['message']
            
            self.logger.debug(f"Updated {replay_id}: {progress*100:.1f}% ({events_processed} events, {bugs_detected} bugs)")
        else:
            self.logger.warning(f"Cannot update progress: session {replay_id} not found")

    async def get_session(self, replay_id: str) -> Optional[ReplaySession]:
        """
        Retrieve a session by replay ID, enriched with current event details.

        Args:
            replay_id: Replay ID to look up.

        Returns:
            ReplaySession object or None if not found.
        """
        session = self.sessions.get(replay_id)
        
        if not session:
            self.logger.warning(f"Session not found for replay {replay_id}")
            return None
        
        # Enrich with current event details – parse raw JSON
        raw_event = session.raw_event_json
        if raw_event:
            try:
                event_json = json.loads(raw_event) if isinstance(raw_event, str) else raw_event
                
                # Infer activity from path
                path_lower = event_json.get('path', '').lower()
                activity_map = {
                    'login': 'User Login',
                    'users': 'User Registration',
                    'basket': 'Cart Update',
                    'products': 'Product Browse',
                    'challenges': 'Scoreboard Check',
                    'address': 'Address Update',
                    'deliverys': 'Delivery Check',
                    'quantitys': 'Quantity Query',
                    'socket.io': 'Real-time Poll',
                    'rest/admin': 'App Config Fetch',
                    'api/cards': 'Payment Info',
                    'wallet': 'Wallet Check',
                }
                inferred_activity = next((v for k, v in activity_map.items() if k in path_lower), 'API Request')
                
                # Set details dict
                session.current_event_details = {
                    'method': event_json.get('method', 'GET'),
                    'path': event_json.get('path', 'Unknown'),
                    'activity': inferred_activity,
                    'status': event_json.get('status', 'N/A')
                }
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                self.logger.warning(f"Failed to parse event JSON for {replay_id}: {e}")
                session.current_event_details = {
                    'method': 'GET', 'path': 'Unknown', 'activity': 'Parse Error', 'status': 'N/A'
                }
        else:
            # Fallback if no raw event
            session.current_event_details = {
                'method': session.current_event_id.split()[0] if session.current_event_id else 'GET',
                'path': 'Unknown',
                'activity': 'N/A',
                'status': 'N/A'
            }
        
        return session

    async def list_sessions(self, status: Optional[str] = None, replay_id: Optional[str] = None) -> List[ReplaySession]:
        """
        List sessions with optional filters.

        Args:
            status: Filter by session status.
            replay_id: Filter by replay ID.

        Returns:
            List of matching ReplaySession objects.
        """
        sessions = list(self.sessions.values())
        if status:
            sessions = [s for s in sessions if s.status == status]
        if replay_id:
            sessions = [s for s in sessions if s.replay_id == replay_id]
        self.logger.debug(f"Listed {len(sessions)} sessions")
        return sessions

    def complete_session(self, replay_id: str):
        """
        Mark session as completed (SYNC - safe to call).
        """
        session = self.sessions.get(replay_id)
        if session:
            session.status = "completed"
            session.progress = 1.0
            self.logger.info(f"Completed session {replay_id}")
        else:
            self.logger.warning(f"Cannot complete: session {replay_id} not found")

    def delete_session(self, replay_id: str):
        """
        Delete a session.
        """
        if replay_id in self.sessions:
            del self.sessions[replay_id]
            self.logger.info(f"Deleted session {replay_id}")
        else:
            self.logger.warning(f"Cannot delete: session {replay_id} not found")