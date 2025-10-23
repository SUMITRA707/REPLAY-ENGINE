# Deterministic Replay Engine (Redis Streams)

A production-ready replay engine that consumes durable real-time logs from Redis Streams, stores them, and provides deterministic replay, debugging, and analysis capabilities.

## Overview

This project consists of two main components:

1. **Universal Logging Hook Sidecar** - A FastAPI microservice that forwards canonical events to Redis Streams
2. **Replay Engine** - A comprehensive system for deterministic replay, bug detection, and analysis

## Features

- **Deterministic Replay**: Canonical ordering based on timestamp + event_id tie-breaker
- **Redis Streams Integration**: Durable, real-time event consumption with consumer groups
- **Checkpoint Management**: Redis-backed checkpointing for resumable replays
- **Bug Detection**: Automated detection of errors, timing gaps, and correlation issues
- **Multiple Replay Modes**: Dry-run, live, and timed replay modes
- **Session Management**: Complete lifecycle management for replay sessions
- **Prometheus Metrics**: Comprehensive observability and monitoring
- **Docker Support**: Full containerization with docker-compose
- **CLI Interface**: Command-line tool for replay operations
- **REST API**: FastAPI-based control API for integration

# (Note: Full README from document here - copy the entire markdown block from the human message.)