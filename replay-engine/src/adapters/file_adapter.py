"""
File adapter for fallback event storage and replay
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class FileEvent:
    """Event stored in file format"""
    event_id: str
    timestamp: str
    session_id: Optional[str]
    request_id: Optional[str]
    source: str
    container: Optional[str]
    level: str
    method: Optional[str]
    path: Optional[str]
    status: Optional[int]
    payload: Dict[str, Any]
    meta: Dict[str, Any]
    stored_at: str


class FileAdapter:
    """File-based adapter for event storage and retrieval"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.events_file = os.path.join(data_dir, "events.jsonl")
        self.index_file = os.path.join(data_dir, "index.json")
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize index if it doesn't exist
        if not os.path.exists(self.index_file):
            self._init_index()
    
    def _init_index(self):
        """Initialize the event index"""
        index = {
            "total_events": 0,
            "sessions": {},
            "sources": {},
            "levels": {},
            "last_updated": datetime.utcnow().isoformat() + "Z"
        }
        
        with open(self.index_file, 'w') as f:
            json.dump(index, f, indent=2)
    
    def _load_index(self) -> Dict[str, Any]:
        """Load the event index"""
        try:
            with open(self.index_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load index: {e}")
            return {"total_events": 0, "sessions": {}, "sources": {}, "levels": {}}
    
    def _save_index(self, index: Dict[str, Any]):
        """Save the event index"""
        try:
            index["last_updated"] = datetime.utcnow().isoformat() + "Z"
            with open(self.index_file, 'w') as f:
                json.dump(index, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save index: {e}")
    
    def store_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Store an event to the file
        
        Args:
            event_data: Event data dictionary
            
        Returns:
            True if stored successfully
        """
        try:
            # Create FileEvent object
            file_event = FileEvent(
                event_id=event_data.get("event_id", ""),
                timestamp=event_data.get("timestamp", ""),
                session_id=event_data.get("session_id"),
                request_id=event_data.get("request_id"),
                source=event_data.get("source", ""),
                container=event_data.get("container"),
                level=event_data.get("level", "INFO"),
                method=event_data.get("method"),
                path=event_data.get("path"),
                status=event_data.get("status"),
                payload=event_data.get("payload", {}),
                meta=event_data.get("meta", {}),
                stored_at=datetime.utcnow().isoformat() + "Z"
            )
            
            # Append to events file
            with open(self.events_file, 'a') as f:
                f.write(json.dumps(asdict(file_event)) + '\n')
            
            # Update index
            index = self._load_index()
            index["total_events"] += 1
            
            # Update session count
            if file_event.session_id:
                index["sessions"][file_event.session_id] = index["sessions"].get(file_event.session_id, 0) + 1
            
            # Update source count
            index["sources"][file_event.source] = index["sources"].get(file_event.source, 0) + 1
            
            # Update level count
            index["levels"][file_event.level] = index["levels"].get(file_event.level, 0) + 1
            
            self._save_index(index)
            
            logger.debug(f"Stored event {file_event.event_id} to file")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store event: {e}")
            return False
    
    def load_events(
        self,
        session_id: Optional[str] = None,
        source: Optional[str] = None,
        level: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[FileEvent]:
        """
        Load events from file with optional filtering
        
        Args:
            session_id: Filter by session ID
            source: Filter by source
            level: Filter by log level
            start_time: Filter by start timestamp (ISO8601)
            end_time: Filter by end timestamp (ISO8601)
            limit: Maximum number of events to return
            
        Returns:
            List of FileEvent objects
        """
        events = []
        
        try:
            if not os.path.exists(self.events_file):
                return events
            
            with open(self.events_file, 'r') as f:
                for line_num, line in enumerate(f):
                    if limit and len(events) >= limit:
                        break
                    
                    try:
                        event_data = json.loads(line.strip())
                        event = FileEvent(**event_data)
                        
                        # Apply filters
                        if session_id and event.session_id != session_id:
                            continue
                        if source and event.source != source:
                            continue
                        if level and event.level != level:
                            continue
                        if start_time and event.timestamp < start_time:
                            continue
                        if end_time and event.timestamp > end_time:
                            continue
                        
                        events.append(event)
                        
                    except Exception as e:
                        logger.warning(f"Failed to parse event on line {line_num + 1}: {e}")
                        continue
            
            logger.info(f"Loaded {len(events)} events from file")
            return events
            
        except Exception as e:
            logger.error(f"Failed to load events: {e}")
            return []
    
    def get_event_stats(self) -> Dict[str, Any]:
        """Get statistics about stored events"""
        index = self._load_index()
        
        return {
            "total_events": index.get("total_events", 0),
            "sessions": len(index.get("sessions", {})),
            "sources": list(index.get("sources", {}).keys()),
            "levels": index.get("levels", {}),
            "last_updated": index.get("last_updated"),
            "file_size_bytes": os.path.getsize(self.events_file) if os.path.exists(self.events_file) else 0
        }
    
    def clear_events(self) -> bool:
        """Clear all stored events"""
        try:
            if os.path.exists(self.events_file):
                os.remove(self.events_file)
            
            self._init_index()
            logger.info("Cleared all events from file storage")
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear events: {e}")
            return False
    
    def export_events(self, output_file: str, format: str = "jsonl") -> bool:
        """
        Export events to a file
        
        Args:
            output_file: Output file path
            format: Export format ("jsonl" or "json")
            
        Returns:
            True if exported successfully
        """
        try:
            events = self.load_events()
            
            if format == "jsonl":
                with open(output_file, 'w') as f:
                    for event in events:
                        f.write(json.dumps(asdict(event)) + '\n')
            elif format == "json":
                with open(output_file, 'w') as f:
                    json.dump([asdict(event) for event in events], f, indent=2)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            logger.info(f"Exported {len(events)} events to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export events: {e}")
            return False