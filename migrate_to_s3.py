import asyncio
import argparse
import os
import mimetypes
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(override=True)

# Important to modify DEV_MODE for script if running locally, or rely on .env
# We need `boto3` for actual upload
from s3 import create_storage_service, S3StorageService
from db import get_pool, close_pool
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("migrate")

async def migrate_file(s3: S3StorageService, local_path: str, key: str, dry_run: bool) -> str:
    """Read local file and upload to S3, returning the key."""
    if not os.path.exists(local_path):
        logger.warning(f"File missing on disk: {local_path}")
        return None
    
    if dry_run:
        logger.info(f"[DRY-RUN] Would upload {local_path} -> s3://{s3._bucket}/{key}")
        return key

    with open(local_path, "rb") as f:
        content = f.read()

    mime_type, _ = mimetypes.guess_type(local_path)
    mime_type = mime_type or "application/octet-stream"

    uploaded_key = await s3.upload(
        key=key,
        content=content,
        content_type=mime_type,
        cache_control="public, max-age=31536000, immutable"
    )
    logger.info(f"Uploaded -> s3://{s3._bucket}/{uploaded_key}")
    return uploaded_key


async def migrate_tenants(conn, s3: S3StorageService, dry_run: bool):
    logger.info("\n--- Migrating Tenants (Logos) ---")
    rows = await conn.fetch("SELECT id, logo_url FROM tenants WHERE logo_url IS NOT NULL")
    for r in rows:
        url = r["logo_url"]
        if "/uploads/" in url:
            filepath = url.split("/uploads/")[1] # e.g. logos/xxx.png
            local_path = f"uploads/{filepath}"
            ext = os.path.splitext(local_path)[1] or ".png"
            new_key = f"tenants/{r['id']}/logo{ext}"
            
            pushed_key = await migrate_file(s3, local_path, new_key, dry_run)
            if pushed_key and not dry_run:
                await conn.execute("UPDATE tenants SET logo_url=$1 WHERE id=$2", pushed_key, r["id"])


async def migrate_widget_configs(conn, s3: S3StorageService, dry_run: bool):
    logger.info("\n--- Migrating Widget Configs (Logos & BG) ---")
    rows = await conn.fetch("SELECT tenant_id, logo_url, bg_image_url FROM widget_configs")
    for r in rows:
        tid = r["tenant_id"]
        # Logo URL
        if r["logo_url"] and "/uploads/" in r["logo_url"]:
            path = r["logo_url"].split("/uploads/")[1]
            ext = os.path.splitext(path)[1] or ".png"
            key = f"tenants/{tid}/logo{ext}"
            pk = await migrate_file(s3, f"uploads/{path}", key, dry_run)
            if pk and not dry_run:
                await conn.execute("UPDATE widget_configs SET logo_url=$1 WHERE tenant_id=$2", pk, tid)
                
        # Bg Image
        if r["bg_image_url"] and "/uploads/" in r["bg_image_url"]:
            path = r["bg_image_url"].split("/uploads/")[1]
            ext = os.path.splitext(path)[1] or ".png"
            import secrets
            key = f"tenants/{tid}/widget/bg_{secrets.token_hex(4)}{ext}"
            pk = await migrate_file(s3, f"uploads/{path}", key, dry_run)
            if pk and not dry_run:
                await conn.execute("UPDATE widget_configs SET bg_image_url=$1 WHERE tenant_id=$2", pk, tid)


async def migrate_kb_products(conn, s3: S3StorageService, dry_run: bool):
    logger.info("\n--- Migrating Products ---")
    rows = await conn.fetch("SELECT id, tenant_id, image_url FROM kb_products WHERE image_url IS NOT NULL")
    for r in rows:
        url = r["image_url"]
        if "/uploads/" in url:
            path = url.split("/uploads/")[1]
            local_path = f"uploads/{path}"
            
            ext = os.path.splitext(local_path)[1] or ".jpg"
            import secrets
            new_key = f"tenants/{r['tenant_id']}/products/{secrets.token_hex(8)}{ext}"
            
            pushed_key = await migrate_file(s3, local_path, new_key, dry_run)
            if pushed_key and not dry_run:
                await conn.execute("UPDATE kb_products SET image_url=$1 WHERE id=$2", pushed_key, r["id"])


async def migrate_ticket_attachments(conn, s3: S3StorageService, dry_run: bool):
    logger.info("\n--- Migrating Ticket Attachments ---")
    rows = await conn.fetch("SELECT id, ticket_id, file_path, filename FROM ticket_attachments")
    for r in rows:
        path = r["file_path"]
        if path and (path.startswith("uploads") or "/uploads/" in path or os.name == 'nt' and "uploads\\" in path):
            local_path = str(Path(path)) # Normalize
            
            ext = os.path.splitext(r["filename"])[1] or ".pdf"
            import secrets
            new_key = f"tickets/{r['ticket_id']}/{secrets.token_hex(4)}_{r['filename']}"
            
            pushed_key = await migrate_file(s3, local_path, new_key, dry_run)
            if pushed_key and not dry_run:
                await conn.execute("UPDATE ticket_attachments SET file_path=$1 WHERE id=$2", pushed_key, r["id"])


async def main():
    parser = argparse.ArgumentParser(description="Migrate local files to S3 bucket.")
    parser.add_argument("--execute", action="store_true", help="Actually move files to S3 and update DB. Without this, only performs a dry run.")
    args = parser.parse_args()
    
    dry_run = not args.execute
    
    if os.getenv("DEV_MODE", "false").lower() == "true":
        logger.warning("DEV_MODE is true. Forced to false for migration script.")
        os.environ["DEV_MODE"] = "false"
        
    s3 = create_storage_service()
    if not isinstance(s3, S3StorageService):
        logger.error("Failed to initialize S3StorageService. Check AWS credentials.")
        return

    logger.info(f"Connected to S3. Bucket: {s3._bucket}")
    pool = await get_pool()
    async with pool.acquire() as conn:
        await migrate_tenants(conn, s3, dry_run)
        await migrate_widget_configs(conn, s3, dry_run)
        await migrate_kb_products(conn, s3, dry_run)
        await migrate_ticket_attachments(conn, s3, dry_run)
        
    await close_pool()
    logger.info("\nMigration complete!")

if __name__ == "__main__":
    asyncio.run(main())
