# Dockerfile for FastAPI
# Used for both local development and Cloud Run deployment
#
# Local development:
#   Use run_api.sh for easiest setup
#
# Cloud Run deployment:
#   Deployed automatically via GitHub Actions workflow

# Use Python 3.10 to avoid end-of-life warnings (3.9 still works but 3.10+ recommended)
FROM python:3.10-slim

WORKDIR /app

# Upgrade pip to latest version to handle newer package versions
RUN pip install --upgrade pip setuptools wheel

# Copy requirements first (for better Docker layer caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Expose port
EXPOSE 8080

# Environment variables (can be overridden at runtime)
ENV GCP_PROJECT_ID=""
ENV BIGQUERY_DATASET="ridesharing_analytics"
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

