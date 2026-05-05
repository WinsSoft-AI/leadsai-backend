#!/bin/sh
set -e

ENV_TYPE=${APP_ENV:-dev}
FILE_NAME="${ENV_TYPE}.env"

# Determine the S3 folder path based on your structure
if [ "$ENV_TYPE" = "prod" ]; then
    S3_PATH="s3://winssoft-leadsai/ma-b/$FILE_NAME"
else
    S3_PATH="s3://winssoft-leadsai/ma-b/dev.env"
fi

echo "Downloading $FILE_NAME from S3..."
aws s3 cp "$S3_PATH" /app/.env

echo "Loading environment variables..."
export $(grep -v '^#' /app/.env | grep '=' | xargs)

echo "Starting main-backend..."
exec uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2 --timeout-keep-alive 30