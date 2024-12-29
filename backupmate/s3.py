import os
import boto3
import logging
from botocore.exceptions import ClientError
from datetime import datetime

logger = logging.getLogger(__name__)

def _get_s3_client(config):
    """Creates an S3 client using the provided configuration."""
    return boto3.client(
        's3',
        aws_access_key_id=config.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=config.get('AWS_SECRET_ACCESS_KEY'),
        region_name=config.get('AWS_REGION')
    )

def upload_directory(local_path, s3_bucket, s3_prefix, config):
    """
    Uploads a local directory to S3.

    Args:
        local_path (str): Path to the local directory to upload
        s3_bucket (str): Name of the S3 bucket
        s3_prefix (str): Prefix (path) in the S3 bucket
        config (dict): Configuration containing AWS credentials

    Returns:
        bool: True on success, False on failure
    """
    if not os.path.exists(local_path):
        logger.error(f"Local directory {local_path} does not exist")
        return False

    try:
        s3_client = _get_s3_client(config)
        
        # Walk through the directory
        for root, _, files in os.walk(local_path):
            for filename in files:
                local_file_path = os.path.join(root, filename)
                # Calculate relative path from base directory
                relative_path = os.path.relpath(local_file_path, local_path)
                # Construct S3 key with prefix
                s3_key = os.path.join(s3_prefix, relative_path).replace('\\', '/')
                
                try:
                    logger.info(f"Uploading {local_file_path} to s3://{s3_bucket}/{s3_key}")
                    s3_client.upload_file(local_file_path, s3_bucket, s3_key)
                except ClientError as e:
                    logger.error(f"Failed to upload {local_file_path} to S3: {str(e)}")
                    return False
        
        logger.info(f"Successfully uploaded directory {local_path} to s3://{s3_bucket}/{s3_prefix}")
        return True
        
    except ClientError as e:
        logger.error(f"Failed to upload directory {local_path} to S3: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error uploading directory to S3: {str(e)}")
        return False

def download_directory(s3_bucket, s3_prefix, local_path, config):
    """
    Downloads a directory from S3.

    Args:
        s3_bucket (str): Name of the S3 bucket
        s3_prefix (str): Prefix (path) in the S3 bucket
        local_path (str): Path to the local directory where files will be downloaded
        config (dict): Configuration containing AWS credentials

    Returns:
        bool: True on success, False on failure
    """
    try:
        s3_client = _get_s3_client(config)
        
        # Create the local directory if it doesn't exist
        os.makedirs(local_path, exist_ok=True)
        
        # List all objects with the given prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                # Get the relative path by removing the prefix
                relative_path = obj['Key'][len(s3_prefix):].lstrip('/')
                if not relative_path:  # Skip if this is the prefix itself
                    continue
                    
                # Construct the local file path
                local_file_path = os.path.join(local_path, relative_path)
                
                # Create directories if they don't exist
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                
                logger.info(f"Downloading s3://{s3_bucket}/{obj['Key']} to {local_file_path}")
                s3_client.download_file(s3_bucket, obj['Key'], local_file_path)
        
        logger.info(f"Successfully downloaded s3://{s3_bucket}/{s3_prefix} to {local_path}")
        return True
        
    except ClientError as e:
        logger.error(f"Failed to download from S3: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error downloading from S3: {str(e)}")
        return False

def list_objects(s3_bucket, prefix, config):
    """
    Lists objects in an S3 bucket with a given prefix.

    Args:
        s3_bucket (str): Name of the S3 bucket
        prefix (str): Prefix to filter objects
        config (dict): Configuration containing AWS credentials

    Returns:
        list: List of object keys, empty list on failure
    """
    try:
        s3_client = _get_s3_client(config)
        objects = []
        
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
            if 'Contents' in page:
                objects.extend([obj['Key'] for obj in page['Contents']])
        
        return objects
        
    except ClientError as e:
        logger.error(f"Failed to list objects in S3: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error listing S3 objects: {str(e)}")
        return []

def upload_file(local_path, s3_bucket, s3_key, config):
    """
    Uploads a single file to S3.

    Args:
        local_path (str): Path to the local file to upload
        s3_bucket (str): Name of the S3 bucket
        s3_key (str): Key (path) in the S3 bucket
        config (dict): Configuration containing AWS credentials

    Returns:
        bool: True on success, False on failure
    """
    if not os.path.exists(local_path):
        logger.error(f"Local file {local_path} does not exist")
        return False

    try:
        s3_client = _get_s3_client(config)
        logger.info(f"Uploading {local_path} to s3://{s3_bucket}/{s3_key}")
        s3_client.upload_file(local_path, s3_bucket, s3_key)
        return True
    except ClientError as e:
        logger.error(f"Failed to upload {local_path} to S3: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error uploading file to S3: {str(e)}")
        return False

def delete_object(s3_bucket, s3_key, config):
    """
    Deletes an object from S3.

    Args:
        s3_bucket (str): Name of the S3 bucket
        s3_key (str): Key (path) of the object to delete
        config (dict): Configuration containing AWS credentials

    Returns:
        bool: True on success, False on failure
    """
    try:
        s3_client = _get_s3_client(config)
        logger.info(f"Deleting s3://{s3_bucket}/{s3_key}")
        s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
        return True
    except ClientError as e:
        logger.error(f"Failed to delete object from S3: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error deleting object from S3: {str(e)}")
        return False

def get_latest_backup_prefix(s3_bucket, prefix, config):
    """
    Retrieves the prefix of the latest backup based on timestamp in the key.

    Args:
        s3_bucket (str): Name of the S3 bucket
        prefix (str): Base prefix to search under
        config (dict): Configuration containing AWS credentials

    Returns:
        str: Latest backup prefix, None if no backups found or on error
    """
    try:
        objects = list_objects(s3_bucket, prefix, config)
        if not objects:
            return None
            
        # Sort objects by name (which includes timestamp) in descending order
        sorted_objects = sorted(objects, reverse=True)
        if sorted_objects:
            # Return the first (most recent) object's full path
            return sorted_objects[0]
            
        return None
        
    except Exception as e:
        logger.error(f"Failed to get latest backup prefix: {str(e)}")
        return None
