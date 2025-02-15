# Build stage
FROM python:3.9-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt

# Final stage
FROM python:3.9-slim

# Create non-root user
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Copy wheels from builder
COPY --from=builder /app/wheels /app/wheels

# Install dependencies
RUN pip install --no-cache /app/wheels/*

# Copy application code
COPY src/api_pipeline /app/api_pipeline/

# Environment configuration
ARG ENVIRONMENT=prod
ENV ENVIRONMENT=${ENVIRONMENT} \
    PYTHONPATH=/app \
    PORT=8080 \
    PYTHONUNBUFFERED=1

# Copy environment-specific configurations
COPY config/${ENVIRONMENT}/ /app/config/

# Switch to non-root user
USER appuser

# Health check configuration
HEALTHCHECK --interval=30s --timeout=3s \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Command to run the application
CMD exec uvicorn api_pipeline.main:app \
    --host 0.0.0.0 \
    --port ${PORT} \
    --workers 2 \
    --log-level ${LOG_LEVEL:-info} 