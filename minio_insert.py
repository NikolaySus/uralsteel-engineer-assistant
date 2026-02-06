from concurrent import futures
from io import BytesIO
import json
import os
import re
import subprocess
import uuid
import asyncio

import httpx
from minio import Minio
from minio.error import S3Error

MINIO_ADDRESS = os.environ.get('MINIO_ADDRESS', '')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', None)
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', None)

SOURCE_DIR = r"C:\Users\gorku\Documents\Технологические письма\hierarchy_trailing_20260126_182731"


def sanitize_bucket_name(bucket_name):
    """Sanitize bucket name to comply with S3 naming rules."""
    # Replace underscores with hyphens
    sanitized = bucket_name.replace('_', '-')
    
    # Ensure it starts and ends with alphanumeric character
    sanitized = sanitized.strip('.-')
    
    # Remove any invalid characters (only allow lowercase alphanumeric and hyphens)
    sanitized = re.sub(r'[^a-z0-9\-]', '', sanitized.lower())
    
    # Ensure length is between 3 and 63 characters
    if len(sanitized) > 63:
        sanitized = sanitized[:63]
    elif len(sanitized) < 3:
        # If too short, pad with default name
        sanitized = sanitized.ljust(3, 'x')
    
    return sanitized


def get_all_pdf_files(source_dir):
    """Get all PDF files from source directory and subdirectories."""
    pdf_files = []
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if file.lower().endswith('.pdf'):
                full_path = os.path.join(root, file)
                # Get relative path from source directory
                rel_path = os.path.relpath(full_path, source_dir)
                pdf_files.append((full_path, rel_path))
    return pdf_files


def normalize_path(path):
    """Normalize path by replacing backslashes with forward slashes."""
    return path.replace('\\', '/')


def upload_file_to_minio(minio_client, bucket_name, file_path, object_name):
    """Upload a single file to MinIO."""
    try:
        with open(file_path, 'rb') as file_data:
            file_stat = os.stat(file_path)
            minio_client.put_object(
                bucket_name,
                object_name,
                file_data,
                file_stat.st_size,
                content_type='application/pdf'
            )
        print(f"Uploaded: {object_name}")
        return True
    except Exception as e:
        print(f"Error uploading {object_name}: {e}")
        return False


def main():
    # Extract bucket name from source directory and sanitize it
    original_bucket_name = os.path.basename(SOURCE_DIR)
    bucket_name = sanitize_bucket_name(original_bucket_name)
    
    print(f"Original directory name: {original_bucket_name}")
    print(f"Sanitized bucket name: {bucket_name}")
    
    # Initialize MinIO client
    if not all([MINIO_ADDRESS, MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
        print("MinIO credentials not provided. Please set MINIO_ADDRESS, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY environment variables.")
        return
    
    minio_client = Minio(
        MINIO_ADDRESS,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=True  # Set to True if using HTTPS
    )
    
    # Create bucket if not exists
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")
        else:
            print(f"Bucket already exists: {bucket_name}")
    except S3Error as e:
        print(f"Error creating bucket {bucket_name}: {e}")
        return  # Exit if bucket creation fails
    
    # Get all PDF files
    print(f"\nScanning for PDF files in: {SOURCE_DIR}")
    pdf_files = get_all_pdf_files(SOURCE_DIR)
    print(f"Found {len(pdf_files)} PDF file(s)")
    
    # Upload files
    uploaded_count = 0
    for full_path, rel_path in pdf_files:
        # Normalize path (replace backslashes with forward slashes)
        object_name = normalize_path(rel_path)
        
        print(f"Processing: {full_path} -> {object_name}")
        if upload_file_to_minio(minio_client, bucket_name, full_path, object_name):
            uploaded_count += 1
    
    print(f"\nUpload complete: {uploaded_count}/{len(pdf_files)} files uploaded successfully")
    
    # List files in bucket for validation
    print(f"\nListing files in bucket '{bucket_name}':")
    try:
        objects = minio_client.list_objects(bucket_name, recursive=True)
        file_count = 0
        for obj in objects:
            print(f"  - {obj.object_name}")
            file_count += 1
        
        if file_count == 0:
            print("  (No files found in bucket)")
    except S3Error as e:
        print(f"Error listing objects: {e}")


if __name__ == "__main__":
    main()