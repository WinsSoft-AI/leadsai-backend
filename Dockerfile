# ═══════════════════════════════════════════════════════════════════════════════
# LeadsAI — Main Backend Dockerfile
# Lightweight: FastAPI + asyncpg + Razorpay + S3
# ═══════════════════════════════════════════════════════════════════════════════
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws/

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py auth.py db.py db_init.py models.py payments.py lead_manager.py ai_proxy.py s3.py ./
COPY favicon.ico ./
COPY widget/ ./widget/

RUN mkdir -p /app/uploads

# Copy entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

ENTRYPOINT ["/entrypoint.sh"]