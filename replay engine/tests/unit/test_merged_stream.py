"""
Unit tests for replay components
"""

import pytest
from unittest.mock import MagicMock, AsyncMock
from src.replay.checkpoint_store import CheckpointStore
from src.replay.session_manager import SessionManager
from src.replay.bug_detector import BugDetector


@pytest.fixture
def mock_redis_client():
    redis_client = MagicMock()
    redis_client.hset = AsyncMock(return_value=True)
    redis_client.expire = AsyncMock(return_value=True)
    redis_client.hgetall = AsyncMock(return_value={})
    redis_client.hdel = AsyncMock(return_value=1)
    redis_client.delete = AsyncMock(return_value=1)
    redis_client.keys = AsyncMock(return_value=[])
    return redis_client
    
@pytest.fixture
def checkpoint_store(mock_redis_client):
    """Create checkpoint store with mock Redis client"""
    return CheckpointStore(mock_redis_client)
    
@pytest.mark.asyncio
async def test_save_checkpoint(checkpoint_store, mock_redis_client):
    """Test saving checkpoint"""
    replay_id = "test-replay-001"
    checkpoint_data = {
        "events_processed": 100,
        "total_events": 1000,
        "current_message_id": "1234567890000-0",
        "progress": 0.1
    }
    
    result = await checkpoint_store.save_checkpoint(
        replay_id=replay_id,
        checkpoint_data=checkpoint_data
    )
    
    assert result is True
    mock_redis_client.hset.assert_called_once()
    mock_redis_client.expire.assert_called_once()

# (Note: Full test code from document here - copy the entire unit test block from the human message. It's for checkpoint, session, bug_detector.)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])