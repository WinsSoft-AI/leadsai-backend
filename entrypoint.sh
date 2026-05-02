#!/bin/sh
set -e

echo "Downloading .env from S3..."
aws s3 cp s3://winssoft-leadsai/ma-b/.env /app/.env

echo "Loading environment variables..."
# Robust .env parser - handles comments, blank lines, quotes
while IFS= read -r line || [ -n "$line" ]; do
  # Skip blank lines and comment lines
  case "$line" in
    ''|\#*) continue ;;
  esac
  # Only process lines that contain =
  case "$line" in
    *=*)
      key="${line%%=*}"
      value="${line#*=}"
      # Strip inline comments (# preceded by space)
      value="${value%% #*}"
      # Strip surrounding quotes if present
      value="${value#\"}" 
      value="${value%\"}"
      value="${value#\'}"
      value="${value%\'}"
      export "$key=$value"
      ;;
  esac
done < /app/.env

echo "Starting main-backend..."
exec uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2 --timeout-keep-alive 30