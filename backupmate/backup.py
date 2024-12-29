import os
import sqlite3
import logging
import shutil
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
        local_path TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        status TEXT NOT NULL
    )
    ''')
    conn.commit()
    return conn

def _clean_backup_chain(config):
    """Clean up the existing backup chain directory."""
    chain_dir = os.path.join(config.get('LOCAL_TEMP_DIR'), 'chain')
    if os.path.exists(chain_dir):
        shutil.rmtree(chain_dir)
    os.makedirs(chain_dir, exist_ok=True)

def get_latest_local_backup(config):
    """Get the latest local backup path from the database."""
    try:
        conn = _init_db(config)
        cursor = conn.cursor()
        cursor.execute('''
        SELECT local_path FROM backups 
        WHERE local_path IS NOT NULL 
        ORDER BY timestamp DESC LIMIT 1
        ''')
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        if conn:
            conn.close()

def perform_full_backup(config):
    """
    Orchestrates the full backup process.
    
    Args:
        config (dict): Configuration parameters
        
    Returns:
        bool: True on success, False on failure
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    temp_dir = os.path.join(config.get('LOCAL_TEMP_DIR'), f'full_{timestamp}')
    chain_dir = os.path.join(config.get('LOCAL_TEMP_DIR'), 'chain')
    backup_dir = os.path.join(chain_dir, f'full_{timestamp}')
    s3_prefix = f"{config.get('FULL_BACKUP_PREFIX')}"
    compressed_file = f"{temp_dir}.tar.gz"
    
    try:
        # Clean up existing backup chain for new full backup
        _clean_backup_chain(config)
        
        # Create backup directories
        os.makedirs(temp_dir, exist_ok=True)
        os.makedirs(backup_dir, exist_ok=True)
        
        # Take full backup
        if not mariadb.take_full_backup(temp_dir, config):
            logger.error("Full backup failed - mariadb-backup command failed")
            # Don't attempt further steps if backup fails
            return False
            
        # Prepare the backup
        if not mariadb.prepare_backup(temp_dir, config=config):
            logger.error("Backup preparation failed - mariadb-backup prepare command failed")
            # Don't attempt further steps if preparation fails
            return False
            
        # Copy prepared backup to chain directory
        shutil.copytree(temp_dir, backup_dir, dirs_exist_ok=True)
        
        # Compress the backup for S3
        if not utils.compress_directory(temp_dir, compressed_file):
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
            
        # Record successful backup with local path
        record_backup_metadata(config, 'full', s3_prefix, backup_dir)
        
        logger.info(f"Full backup completed successfully: {s3_prefix}")
        return True
        
    except Exception as e:
        logger.error(f"Full backup failed: {str(e)}")
        return False
    finally:
        # Cleanup
        logger.debug("Starting cleanup...")
        
        # Clean up temporary directory
        logger.debug(f"Checking temp_dir existence: {temp_dir} - {os.path.exists(temp_dir)}")
        if os.path.exists(temp_dir):
            try:
                logger.debug(f"Removing temporary directory: {temp_dir}")
                shutil.rmtree(temp_dir)
            except OSError as e:
                logger.warning(f"Failed to remove temporary directory: {str(e)}")
        
        # Clean up compressed file
        logger.debug(f"Checking compressed file existence: {compressed_file} - {os.path.exists(compressed_file)}")
        if os.path.exists(compressed_file):
            try:
                logger.debug(f"Removing compressed file: {compressed_file}")
                os.remove(compressed_file)
            except OSError as e:
                logger.warning(f"Failed to remove compressed file: {str(e)}")

def perform_incremental_backup(config, base_prefix):
    """
    Orchestrates the incremental backup process.
    
    Args:
        config (dict): Configuration parameters
        base_prefix (str): S3 prefix of the base backup (not used anymore, kept for compatibility)
        
    Returns:
        bool: True on success, False on failure
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    temp_dir = os.path.join(config.get('LOCAL_TEMP_DIR'), f'inc_{timestamp}')
    chain_dir = os.path.join(config.get('LOCAL_TEMP_DIR'), 'chain')
    backup_dir = os.path.join(chain_dir, f'inc_{timestamp}')
    s3_prefix = f"{config.get('INCREMENTAL_BACKUP_PREFIX')}"
    compressed_file = f"{temp_dir}.tar.gz"
    
    try:
        # Create backup directories
        os.makedirs(temp_dir, exist_ok=True)
        os.makedirs(backup_dir, exist_ok=True)
        
        # Get the latest local backup to use as base
        base_dir = get_latest_local_backup(config)
        if not base_dir or not os.path.exists(base_dir):
            logger.error("No local backup found to base incremental on")
            return False
            
        # Take incremental backup using local base
        if not mariadb.take_incremental_backup(temp_dir, base_dir, config):
            logger.error("Incremental backup failed")
            return False
            
        # Copy prepared backup to chain directory
        shutil.copytree(temp_dir, backup_dir, dirs_exist_ok=True)
        
        # Compress the backup for S3
        if not utils.compress_directory(temp_dir, compressed_file):
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
            
        # Record successful backup with local path
        record_backup_metadata(config, 'incremental', s3_prefix, backup_dir)
        
        logger.info(f"Incremental backup completed successfully: {s3_prefix}")
        return True
        
    except Exception as e:
        logger.error(f"Incremental backup failed: {str(e)}")
        return False
    finally:
        # Cleanup
        logger.debug("Starting cleanup...")
        
        # Clean up temporary directory
        logger.debug(f"Checking temp_dir existence: {temp_dir} - {os.path.exists(temp_dir)}")
        if os.path.exists(temp_dir):
            try:
                logger.debug(f"Removing temporary directory: {temp_dir}")
                shutil.rmtree(temp_dir)
            except OSError as e:
                logger.warning(f"Failed to remove temporary directory: {str(e)}")
        
        # Clean up compressed file
        logger.debug(f"Checking compressed file existence: {compressed_file} - {os.path.exists(compressed_file)}")
        if os.path.exists(compressed_file):
            try:
                logger.debug(f"Removing compressed file: {compressed_file}")
                os.remove(compressed_file)
            except OSError as e:
                logger.warning(f"Failed to remove compressed file: {str(e)}")

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

def record_backup_metadata(config, backup_type, backup_prefix, local_path=None):
    """
    Records backup metadata to the database.
    
    Args:
        config (dict): Configuration parameters
        backup_type (str): Type of backup ('full' or 'incremental')
        backup_prefix (str): S3 prefix of the backup
        local_path (str, optional): Path to the local backup directory
        
    Returns:
        bool: True on success, False on failure
    """
    try:
        conn = _init_db(config)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO backups (backup_type, backup_prefix, local_path, status)
        VALUES (?, ?, ?, ?)
        ''', (backup_type, backup_prefix, local_path, 'success'))
        
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"Failed to record backup metadata: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()
