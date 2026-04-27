#!/bin/sh
set -e

echo "Downloading .env from S3..."
aws s3 cp s3://winssoft-leadsai/ma-b/.env /app/.env

echo "Loading environment variables..."
export $(grep -v '^#' /app/.env | grep '=' | xargs)

echo "Starting main-backend..."
exec uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2 --timeout-keep-alive 30