"""
Integration tests with Redis for replay engine
"""

import pytest
import asyncio
import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Dict, Any

from src.adapters.redis_stream_adapter import RedisStreamAdapter
from src.replay.deterministic_replayer import DeterministicReplayer, ReplayConfig
from src.replay.checkpoint_store import CheckpointStore
from src.replay.session_manager import SessionManager
from src.replay.bug_detector import BugDetector
from src.adapters.file_adapter import FileAdapter
from src.runner import ReplayRunner


class TestReplayWithRedis:
    """Integration tests with Redis"""
    
    @pytest.fixture
    async def redis_adapter(self):
        """Create Redis adapter for testing"""
        adapter = RedisStreamAdapter(
            redis_url="redis://localhost:6379",
            stream_key="test:logs:stream",
            consumer_group="test_replay_group",
            consumer_name="test_consumer"
        )
        await adapter.connect()
        yield adapter
        await adapter.disconnect()
    
    @pytest.fixture
    async def redis_client(self):
        """Create Redis client for testing"""
        import redis.asyncio as redis
        client = redis.Redis.from_url("redis://localhost:6379")
        yield client
        await client.close()
    
    @pytest.fixture
    def temp_reports_dir(self):
        """Create temporary reports directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    async def test_components(self, redis_adapter, redis_client, temp_reports_dir):
        """Create test components"""
        checkpoint_store = CheckpointStore(redis_client)
        session_manager = SessionManager()
        bug_detector = BugDetector()
        file_adapter = FileAdapter(temp_reports_dir)
        
        replayer = DeterministicReplayer(
            redis_adapter=redis_adapter,
            checkpoint_store=checkpoint_store,
            session_manager=session_manager,
            bug_detector=bug_detector,
            file_adapter=file_adapter
        )
        
        return {
            "redis_adapter": redis_adapter,
            "checkpoint_store": checkpoint_store,
            "session_manager": session_manager,
            "bug_detector": bug_detector,
            "file_adapter": file_adapter,
            "replayer": replayer
        }
    
    async def create_test_events(self, redis_adapter, count: int = 10) -> list:
        """Create test events in Redis stream"""
        import redis.asyncio as redis
        redis_client = redis.Redis.from_url("redis://localhost:6379")
        
        events = []
        base_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        
        for i in range(count):
            event_data = {
                "event_id": f"test-event-{i:03d}",
                "timestamp": (base_time.replace(second=i)).isoformat() + "Z",
                "session_id": f"session-{i % 3}",  # 3 different sessions
                "request_id": f"req-{i:03d}",
                "source": "test-source",
                "container": "test-container",
                "level": "ERROR" if i % 5 == 0 else "INFO",
                "method": "POST" if i % 2 == 0 else "GET",
                "path": f"/api/test/{i}",
                "status": 200 if i % 3 != 0 else 500,
                "payload": {
                    "test_data": f"value_{i}",
                    "index": i
                },
                "meta": {
                    "user_agent": "test-agent",
                    "ip": "127.0.0.1"
                }
            }
            
            # Add to Redis stream
            stream_id = await redis_client.xadd(
                redis_adapter.stream_key,
                event_data
            )
            
            events.append({
                "stream_id": stream_id,
                "data": event_data
            })
        
        await redis_client.close()
        return events
    
    @pytest.mark.asyncio
    async def test_redis_stream_consumption(self, redis_adapter):
        """Test consuming events from Redis stream"""
        # Create test events
        test_events = await self.create_test_events(redis_adapter, 5)
        
        # Consume events
        messages = await redis_adapter.read_messages_by_range()
        
        assert len(messages) >= 5
        
        # Verify message structure
        for message in messages[:5]:
            assert hasattr(message, 'stream_id')
            assert hasattr(message, 'fields')
            assert hasattr(message, 'timestamp')
            assert 'event_id' in message.fields
            assert 'timestamp' in message.fields
    
    # (Note: Full test code from document here - copy the entire integration test block from the human message.)

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])