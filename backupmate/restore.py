import os
import logging
import subprocess
from . import s3
from . import mariadb

logger = logging.getLogger(__name__)

def restore_specific_backup(backup_identifier, restore_method, config):
    """
    Orchestrates the restore process for a specified backup.

    Args:
        backup_identifier (str): S3 prefix of the backup to restore
        restore_method (str): Method to use for restore ('copy-back' or 'move-back')
        config (dict): Configuration parameters

    Returns:
        bool: True on success, False on failure
    """
    if restore_method not in ['copy-back', 'move-back']:
        logger.error(f"Invalid restore method: {restore_method}")
        return False

    try:
        # Create staging directory
        local_staging_dir = config.get('LOCAL_TEMP_DIR', '/tmp/backupmate_restore')
        os.makedirs(local_staging_dir, exist_ok=True)

        # Download and prepare backup
        if not download_and_prepare_backup(backup_identifier, local_staging_dir, config):
            logger.error("Failed to download and prepare backup")
            return False

        # Stop MariaDB server
        if not stop_mariadb_server():
            logger.error("Failed to stop MariaDB server")
            return False

        try:
            # Restore backup
            if not mariadb.restore_backup(local_staging_dir, config, method=restore_method):
                logger.error("Failed to restore backup")
                return False

            logger.info("Backup restored successfully")
            return True

        finally:
            # Always attempt to start MariaDB server, even if restore failed
            if not start_mariadb_server():
                logger.error("Failed to start MariaDB server")
                return False

    except Exception as e:
        logger.error(f"Unexpected error during restore: {str(e)}")
        return False

def download_and_prepare_backup(backup_prefix, local_staging_dir, config):
    """
    Downloads the necessary backup files from S3 and prepares them.

    Args:
        backup_prefix (str): S3 prefix of the backup to download
        local_staging_dir (str): Local directory to store downloaded files
        config (dict): Configuration parameters

    Returns:
        bool: True on success, False on failure
    """
    try:
        s3_bucket = config.get('S3_BUCKET_NAME')
        if not s3_bucket:
            logger.error("S3_BUCKET_NAME not found in config")
            return False

        # Download backup files
        if not s3.download_directory(s3_bucket, backup_prefix, local_staging_dir, config):
            logger.error("Failed to download backup files from S3")
            return False

        # Prepare the backup
        if not mariadb.prepare_backup(local_staging_dir, config=config):
            logger.error("Failed to prepare backup")
            return False

        logger.info("Backup downloaded and prepared successfully")
        return True

    except Exception as e:
        logger.error(f"Unexpected error during backup download and preparation: {str(e)}")
        return False

def stop_mariadb_server():
    """
    Stops the MariaDB server.

    Returns:
        bool: True on success, False on failure
    """
    try:
        # Using systemctl for Linux systems
        subprocess.run(['systemctl', 'stop', 'mariadb'], check=True, capture_output=True)
        logger.info("MariaDB server stopped successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to stop MariaDB server: {e}")
        if e.stdout:
            logger.error(f"Stdout: {e.stdout.decode()}")
        if e.stderr:
            logger.error(f"Stderr: {e.stderr.decode()}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error stopping MariaDB server: {str(e)}")
        return False

def start_mariadb_server():
    """
    Starts the MariaDB server.

    Returns:
        bool: True on success, False on failure
    """
    try:
        # Using systemctl for Linux systems
        subprocess.run(['systemctl', 'start', 'mariadb'], check=True, capture_output=True)
        logger.info("MariaDB server started successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start MariaDB server: {e}")
        if e.stdout:
            logger.error(f"Stdout: {e.stdout.decode()}")
        if e.stderr:
            logger.error(f"Stderr: {e.stderr.decode()}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error starting MariaDB server: {str(e)}")
        return False
