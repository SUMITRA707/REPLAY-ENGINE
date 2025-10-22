from typing import List, Dict, Optional
from datetime import datetime, timezone
from dataclasses import dataclass
from ..common.logging_config import ReplayLogger
import logging

@dataclass
class ReplaySession:
    session_id: str
    replay_id: str
    config: Dict
    status: str = "pending"
    started_at: Optional[datetime] = None
    events_processed: int = 0
    total_events: int = 0
    progress: float = 0.0

class SessionManager:
    def __init__(self):
        self.sessions: Dict[str, ReplaySession] = {}
        self.logger = ReplayLogger(__name__)

    async def create_session(self, replay_id: str, config: Dict) -> ReplaySession:
        """
        Create a new replay session.

        Args:
            replay_id: Unique identifier for the replay.
            config: Configuration for the session.

        Returns:
            Created ReplaySession object.
        """
        session_id = f"s-{len(self.sessions) + 1}"
        session = ReplaySession(
            session_id=session_id,
            replay_id=replay_id,
            config=config
        )
        self.sessions[session_id] = session
        self.logger.info(f"Created session {session_id} for replay {replay_id}")
        return session

    async def update_session_status(self, replay_id: str, status: str, **kwargs) -> bool:
        """
        Update the status of a session.

        Args:
            replay_id: Replay ID to update.
            status: New status (pending, running, stopped, completed, failed).
            **kwargs: Additional session attributes to update.

        Returns:
            True if updated successfully, False otherwise.
        """
        for session in self.sessions.values():
            if session.replay_id == replay_id:
                session.status = status
                session.started_at = datetime.now(timezone.utc) if status == "running" else session.started_at
                for key, value in kwargs.items():
                    setattr(session, key, value)
                self.logger.info(f"Updated session {session.session_id} status to {status}")
                return True
        self.logger.warning(f"Session not found for replay {replay_id}")
        return False

    async def update_session_progress(self, replay_id: str, **kwargs) -> bool:
        """
        Update progress metrics for a session.

        Args:
            replay_id: Replay ID to update.
            **kwargs: Progress metrics (events_processed, total_events, progress).

        Returns:
            True if updated successfully, False otherwise.
        """
        for session in self.sessions.values():
            if session.replay_id == replay_id:
                session.events_processed = kwargs.get("events_processed", session.events_processed)
                session.total_events = kwargs.get("total_events", session.total_events)
                session.progress = kwargs.get("progress", session.progress)
                self.logger.debug(f"Updated progress for session {session.session_id}: {session.progress}")
                return True
        self.logger.warning(f"Session not found for replay {replay_id}")
        return False

    async def get_session(self, replay_id: str) -> Optional[ReplaySession]:
        """
        Retrieve a session by replay ID.

        Args:
            replay_id: Replay ID to look up.

        Returns:
            ReplaySession object or None if not found.
        """
        for session in self.sessions.values():
            if session.replay_id == replay_id:
                return session
        self.logger.warning(f"Session not found for replay {replay_id}")
        return None

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