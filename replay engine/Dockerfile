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

# Expose ports for API and metrics
EXPOSE 8000 8050

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["gunicorn", "--worker-class=uvicorn.workers.UvicornWorker", "src.api.control_api:app", "--bind=0.0.0.0:8000", "--workers=2"]