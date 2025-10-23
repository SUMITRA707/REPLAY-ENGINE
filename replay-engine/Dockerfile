FROM python:3.11-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 replay && chown -R replay:replay /app
USER replay

# Expose ports
EXPOSE 8000 8050

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s \
  CMD curl -f http://localhost:8000/health || exit 1

# Run API & dashboard in parallel
CMD ["sh", "-c", "uvicorn src.api.control_api:app --host 0.0.0.0 --port 8000 --workers 2 & python src/dashboard/server.py"]