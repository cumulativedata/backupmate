import os
import sqlite3
import logging
from datetime import datetime
from . import mariadb
from . import s3
from . import utils

logger = logging.getLogger(__name__)

def _init_db(config):
    """Initialize SQLite database for backup metadata."""
    db_path = os.path.join(config.get('LOCAL_TEMP_DIR'), 'backups.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS backups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        backup_type TEXT NOT NULL,
        backup_prefix TEXT NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        status TEXT NOT NULL
    )
    ''')
    conn.commit()
    return conn

def perform_full_backup(config):
    """
    Orchestrates the full backup process.
    
    Args:
        config (dict): Configuration parameters
        
    Returns:
        bool: True on success, False on failure
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join(config.get('LOCAL_TEMP_DIR'), f'full_{timestamp}')
    s3_prefix = f"{config.get('FULL_BACKUP_PREFIX')}"
    compressed_file = f"{backup_dir}.tar.gz"
    
    try:
        # Create backup directory
        os.makedirs(backup_dir, exist_ok=True)
        
        # Take full backup
        if not mariadb.take_full_backup(backup_dir, config):
            logger.error("Full backup failed - mariadb-backup command failed")
            # Don't attempt further steps if backup fails
            return False
            
        # Prepare the backup
        if not mariadb.prepare_backup(backup_dir, config=config):
            logger.error("Backup preparation failed - mariadb-backup prepare command failed")
            # Don't attempt further steps if preparation fails
            return False
            
        # Compress the backup
        if not utils.compress_directory(backup_dir, compressed_file):
            logger.error("Backup compression failed")
            return False
            
        # Upload to S3
        if not s3.upload_file(
            compressed_file,
            config.get('S3_BUCKET_NAME'),
            s3_prefix,
            config
        ):
            logger.error("S3 upload failed")
            return False
            
        # Record successful backup
        record_backup_metadata(config, 'full', s3_prefix)
        
        logger.info(f"Full backup completed successfully: {s3_prefix}")
        return True
        
    except Exception as e:
        logger.error(f"Full backup failed: {str(e)}")
        return False
    finally:
        # Cleanup
        try:
            # Use shutil.rmtree for backup_dir since it may not be empty
            if os.path.exists(backup_dir):
                import shutil
                shutil.rmtree(backup_dir)
            if os.path.exists(compressed_file):
                os.remove(compressed_file)
        except OSError as e:
            logger.warning(f"Cleanup failed: {str(e)}")
            # Continue even if cleanup fails - it's not critical

def perform_incremental_backup(config, base_prefix):
    """
    Orchestrates the incremental backup process.
    
    Args:
        config (dict): Configuration parameters
        base_prefix (str): S3 prefix of the base backup
        
    Returns:
        bool: True on success, False on failure
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join(config.get('LOCAL_TEMP_DIR'), f'inc_{timestamp}')
    base_dir = os.path.join(config.get('LOCAL_TEMP_DIR'), 'base_backup')
    s3_prefix = f"{config.get('INCREMENTAL_BACKUP_PREFIX')}"
    compressed_file = f"{backup_dir}.tar.gz"
    
    try:
        # Create backup directories
        os.makedirs(backup_dir, exist_ok=True)
        os.makedirs(base_dir, exist_ok=True)
        
        # Download base backup
        if not s3.download_directory(
            config.get('S3_BUCKET_NAME'),
            base_prefix,
            base_dir,
            config
        ):
            logger.error("Failed to download base backup")
            return False
            
        # Take incremental backup
        if not mariadb.take_incremental_backup(backup_dir, base_dir, config):
            logger.error("Incremental backup failed")
            return False
            
        # Compress the backup
        if not utils.compress_directory(backup_dir, compressed_file):
            logger.error("Backup compression failed")
            return False
            
        # Upload to S3
        if not s3.upload_file(
            compressed_file,
            config.get('S3_BUCKET_NAME'),
            s3_prefix,
            config
        ):
            logger.error("S3 upload failed")
            return False
            
        # Record successful backup
        record_backup_metadata(config, 'incremental', s3_prefix)
        
        logger.info(f"Incremental backup completed successfully: {s3_prefix}")
        return True
        
    except Exception as e:
        logger.error(f"Incremental backup failed: {str(e)}")
        return False
    finally:
        # Cleanup
        try:
            # Use shutil.rmtree for directories since they may not be empty
            if os.path.exists(backup_dir):
                import shutil
                shutil.rmtree(backup_dir)
            if os.path.exists(base_dir):
                shutil.rmtree(base_dir)
            if os.path.exists(compressed_file):
                os.remove(compressed_file)
        except OSError as e:
            logger.warning(f"Cleanup failed: {str(e)}")
            # Continue even if cleanup fails - it's not critical

def get_latest_full_backup_prefix(s3_bucket, full_backup_prefix, config):
    """
    Retrieves the S3 prefix of the latest successful full backup.
    
    Args:
        s3_bucket (str): S3 bucket name
        full_backup_prefix (str): Prefix for full backups
        config (dict): Configuration parameters
        
    Returns:
        str: Latest full backup prefix, None if no backups found
    """
    try:
        return s3.get_latest_backup_prefix(s3_bucket, full_backup_prefix, config)
    except Exception as e:
        logger.error(f"Failed to get latest full backup prefix: {str(e)}")
        return None

def list_backups_from_db(config, output_json=False):
    """
    Lists available backups from the metadata database.
    
    Args:
        config (dict): Configuration parameters
        output_json (bool): Whether to return JSON format
        
    Returns:
        list: List of backup records
    """
    try:
        conn = _init_db(config)
        cursor = conn.cursor()
        cursor.execute('''
        SELECT backup_type, backup_prefix, timestamp, status
        FROM backups
        ORDER BY timestamp DESC
        ''')
        backups = cursor.fetchall()
        
        if output_json:
            import json
            return json.dumps([{
                'type': b[0],
                'prefix': b[1],
                'timestamp': b[2],
                'status': b[3]
            } for b in backups])
            
        return backups
        
    except Exception as e:
        logger.error(f"Failed to list backups: {str(e)}")
        return [] if not output_json else "[]"
    finally:
        if conn:
            conn.close()

def record_backup_metadata(config, backup_type, backup_prefix):
    """
    Records backup metadata to the database.
    
    Args:
        config (dict): Configuration parameters
        backup_type (str): Type of backup ('full' or 'incremental')
        backup_prefix (str): S3 prefix of the backup
        
    Returns:
        bool: True on success, False on failure
    """
    try:
        conn = _init_db(config)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO backups (backup_type, backup_prefix, status)
        VALUES (?, ?, ?)
        ''', (backup_type, backup_prefix, 'success'))
        
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"Failed to record backup metadata: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()
