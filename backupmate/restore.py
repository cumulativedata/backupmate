import os
import time
import logging
import subprocess
from typing import Union
from . import s3
from . import mariadb
from . import utils
from . import config as config_module

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

        prepared_folder = download_and_prepare_backup(backup_identifier, local_staging_dir, config)
        # Download and prepare backup
        if not prepared_folder:
            logger.error("Failed to download and prepare backup")
            return False

        # Stop MariaDB server
        if not stop_mariadb_server():
            logger.error("Failed to stop MariaDB server")
            return False

        try:
            # Restore backup
            if not mariadb.restore_backup(prepared_folder, config, method=restore_method):
                logger.error("Failed to restore backup")
                return False

            logger.info("Backup restored successfully")
            return True

        finally:
            # Always attempt to start MariaDB server, even if restore failed
            if not start_mariadb_server():
                logger.error("Failed to start MariaDB server")
                return False

            # In test environment, skip server accessibility check
            if not config.get('IS_TEST'):
                # Wait for server to be accessible
                max_retries = 30
                retry_interval = 1
                server_started = False
                
                for _ in range(max_retries):
                    try:
                        subprocess.run(
                            [
                                'mariadb',
                                f'--socket={config.get("MARIADB_SOCKET")}',
                                '--user=root',
                                '-e', 'SELECT 1'
                            ],
                            check=True,
                            capture_output=True
                        )
                        server_started = True
                        logger.info("MariaDB server is accessible after restore")
                        break
                    except subprocess.CalledProcessError:
                        logger.info("Waiting for MariaDB server to be accessible...")
                        time.sleep(retry_interval)
                        
                if not server_started:
                    log_file = config.get('MARIADB_DATADIR') + '.log'
                    if os.path.exists(log_file):
                        with open(log_file, 'r') as f:
                            log_content = f.read()
                        logger.error(f"MariaDB Error Log:\n{log_content}")
                    logger.error("Failed to access MariaDB server after restore")
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
        str: Path to prepared backup directory on success, False on failure
    """
    try:
        s3_bucket = config.get('S3_BUCKET_NAME')
        if not s3_bucket:
            logger.error("S3_BUCKET_NAME not found in config")
            return False

        # Create a temporary directory for the compressed file
        temp_dir = os.path.join(local_staging_dir, 'temp')
        os.makedirs(temp_dir, exist_ok=True)

        # Get the tar.gz filename from the backup prefix
        tar_filename = os.path.basename(backup_prefix.rstrip('/'))
        tar_path = os.path.join(temp_dir, tar_filename)

        # Download the backup file directly
        logger.info(f"Attempting to download backup from s3://{s3_bucket}/{backup_prefix}")
        if not s3.download_file(s3_bucket, backup_prefix, tar_path, config):
            logger.error("Failed to download backup file from S3")
            return False

        if not os.path.exists(tar_path):
            logger.error(f"Downloaded file not found at expected path: {tar_path}")
            return False
        extract_dir = os.path.join(local_staging_dir, 'extracted')

        # Extract the backup
        backup_dir = utils.decompress_archive(tar_path, extract_dir)
        if not backup_dir:
            logger.error("Failed to extract backup archive")
            return False

        # Prepare the backup
        if not mariadb.prepare_backup(backup_dir, config=config):
            logger.error("Failed to prepare backup")
            return False

        logger.info("Backup downloaded and prepared successfully")
        return backup_dir

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
        config = config_module.load_config()
        if config.get('MYSQL_STOP_COMMAND'):
            # Use configured command
            subprocess.run(config['MYSQL_STOP_COMMAND'], shell=True, check=True)
        else:
            # Default to systemctl
            subprocess.run(['systemctl', 'stop', 'mariadb'], check=True, capture_output=True )
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
        config = config_module.load_config()
        if config.get('MYSQL_START_COMMAND'):
            # Use configured command
            subprocess.run(config['MYSQL_START_COMMAND'], shell=True, check=True)
        else:
            # Default to systemctl
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
