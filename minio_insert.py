from concurrent import futures
from io import BytesIO
import json
import os
import re
import subprocess
import uuid
import asyncio
import argparse

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


def get_all_files(source_dir, extension):
    """Get all files with specified extension from source directory and subdirectories."""
    files = []
    ext = f'.{extension}' if not extension.startswith('.') else extension
    for root, dirs, filenames in os.walk(source_dir):
        for file in filenames:
            if file.lower().endswith(ext.lower()):
                full_path = os.path.join(root, file)
                # Get relative path from source directory
                rel_path = os.path.relpath(full_path, source_dir)
                files.append((full_path, rel_path))
    return files


def normalize_path(path):
    """Normalize path by replacing backslashes with forward slashes."""
    return path.replace('\\', '/')


def upload_file_to_minio(minio_client, bucket_name, file_path, object_name, content_type):
    """Upload a single file to MinIO."""
    try:
        with open(file_path, 'rb') as file_data:
            file_stat = os.stat(file_path)
            minio_client.put_object(
                bucket_name,
                object_name,
                file_data,
                file_stat.st_size,
                content_type=content_type
            )
        print(f"Uploaded: {object_name}")
        return True
    except Exception as e:
        print(f"Error uploading {object_name}: {e}")
        return False


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Upload files to MinIO based on file extension')
    parser.add_argument('extension', choices=['pdf', 'md'], help='File extension to upload (pdf or md)')
    args = parser.parse_args()
    
    file_extension = args.extension
    
    # Set content type based on extension
    content_type_map = {
        'pdf': 'application/pdf',
        'md': 'text/markdown'
    }
    content_type = content_type_map[file_extension]
    
    # Extract bucket name from source directory and sanitize it
    original_bucket_name = os.path.basename(SOURCE_DIR)
    bucket_name = sanitize_bucket_name(original_bucket_name)
    
    print(f"Original directory name: {original_bucket_name}")
    print(f"Sanitized bucket name: {bucket_name}")
    print(f"File extension to upload: .{file_extension}\n")
    
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
    
    # Get all files with specified extension
    print(f"\nScanning for .{file_extension} files in: {SOURCE_DIR}")
    files = get_all_files(SOURCE_DIR, file_extension)
    print(f"Found {len(files)} .{file_extension} file(s)")
    
    # Upload files
    uploaded_count = 0
    for full_path, rel_path in files:
        # Normalize path (replace backslashes with forward slashes)
        object_name = normalize_path(rel_path)
        
        print(f"Processing: {full_path} -> {object_name}")
        if upload_file_to_minio(minio_client, bucket_name, full_path, object_name, content_type):
            uploaded_count += 1
    
    print(f"\nUpload complete: {uploaded_count}/{len(files)} files uploaded successfully")
    
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
