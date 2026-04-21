# ═══════════════════════════════════════════════════════════════════════════════
# OmniChat — Main Backend Dockerfile
# Lightweight: FastAPI + asyncpg + Razorpay + S3
# ═══════════════════════════════════════════════════════════════════════════════

FROM python:3.11-slim

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py auth.py db_init.py models.py payments.py lead_manager.py ai_proxy.py s3.py ./
COPY favicon.ico ./
COPY widget/ ./widget/

# Create uploads directory
RUN mkdir -p /app/uploads

# Expose the port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run with uvicorn (production: no --reload, multiple workers)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--timeout-keep-alive", "30"]
