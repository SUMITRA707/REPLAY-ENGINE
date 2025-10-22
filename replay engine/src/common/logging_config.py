"""
Common logging configuration for the replay engine
"""

import logging
import json
import sys
from typing import Any, Dict
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in log_entry and not key.startswith('_'):
                log_entry[key] = value
        
        return json.dumps(log_entry)


def setup_logging(level: str = "INFO", json_format: bool = True) -> None:
    """
    Setup logging configuration for the replay engine
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: Whether to use JSON formatting
    """
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))
    
    # Set formatter
    if json_format:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Configure specific loggers
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("redis").setLevel(logging.WARNING)
    logging.getLogger("aioredis").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the given name"""
    return logging.getLogger(name)


class ReplayLogger:
    """Contextual logger for replay operations"""
    
    def __init__(self, name: str, replay_id: str = None):
        self.logger = logging.getLogger(name)
        self.replay_id = replay_id
        self._extra = {"replay_id": replay_id} if replay_id else {}
    
    def info(self, message: str, **kwargs):
        """Log info message with replay context"""
        extra = {**self._extra, **kwargs}
        self.logger.info(message, extra=extra)
    
    def error(self, message: str, **kwargs):
        """Log error message with replay context"""
        extra = {**self._extra, **kwargs}
        self.logger.error(message, extra=extra)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with replay context"""
        extra = {**self._extra, **kwargs}
        self.logger.warning(message, extra=extra)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with replay context"""
        extra = {**self._extra, **kwargs}
        self.logger.debug(message, extra=extra)
    
    def set_replay_id(self, replay_id: str):
        """Update replay ID context"""
        self.replay_id = replay_id
        self._extra["replay_id"] = replay_id