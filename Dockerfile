# Multi-stage Dockerfile for Stock Evaluation Pipeline
# Stage 1: Base image with Python and system dependencies
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Stage 2: Dependencies installation
FROM base as dependencies

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Stage 3: Production image
FROM base as production

# Set working directory
WORKDIR /app

# Copy Python dependencies from dependencies stage
COPY --from=dependencies /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=dependencies /usr/local/bin /usr/local/bin

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p data logs reports && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Default command
CMD ["python", "pipeline/run_pipeline.py", "--help"]

# Stage 4: Development image
FROM base as development

# Set working directory
WORKDIR /app

# Install development dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install additional development tools
RUN pip install --no-cache-dir \
    ipython \
    jupyter \
    black \
    flake8 \
    mypy

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p data logs reports && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose port for Jupyter (if needed)
EXPOSE 8888

# Default command for development
CMD ["python", "-m", "ipython"] 