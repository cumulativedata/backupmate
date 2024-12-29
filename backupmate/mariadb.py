import subprocess
import logging
import os
import shutil
import time

logger = logging.getLogger(__name__)

def verify_test_instance(config):
    """
    Verifies if this is the test instance by checking for test_db database.
    Only used during integration testing.

    Args:
        config (dict): The configuration parameters.

    Returns:
        bool: True if test instance is detected, False otherwise.
    """
    command = [
        'mariadb',
        '--user=root',
        '-e', 'SHOW DATABASES LIKE "test_db";'
    ]
    
    # Add socket file if specified
    if config.get('MARIADB_SOCKET'):
        command.append(f"--socket={config['MARIADB_SOCKET']}")
        
    print(f"Executing command: {' '.join(command)}")
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return 'test_db' in result.stdout
    except subprocess.CalledProcessError:
        return False

def take_full_backup(target_dir, config):
    """
    Executes a full Mariabackup using the provided configuration.

    Args:
        target_dir (str): The directory to store the backup.
        config (dict): The configuration parameters.

    Returns:
        bool: True on success, False on failure.
    """
    # Log integration test status prominently
    if config.get('IS_INTEGRATION_TEST'):
        # Use print for better visibility
        print("="*50)
        print("=== RUNNING IN INTEGRATION TEST MODE ===")
        print("="*50)
        # Ensure socket file is specified for test instance
        if not config.get('MARIADB_SOCKET'):
            logger.error("MARIADB_SOCKET must be specified for integration testing")
            return False
        # Verify we're backing up the test instance
        if not verify_test_instance(config):
            logger.error("Integration test verification failed: test_db not found. Are we connected to the right instance?")
            return False
    mariadb_backup_path = config.get('MARIADB_BACKUP_PATH')
    db_host = config.get('DB_HOST')
    db_port = config.get('DB_PORT')
    db_user = config.get('DB_USER')
    db_password = config.get('DB_PASSWORD')

    # Build base command with common options
    command = [
        mariadb_backup_path,
        "--backup",
        f"--target-dir={target_dir}",
        f"--host={db_host}",
        f"--port={db_port}",
        f"--user={db_user}",
        f"--password={db_password}",
    ]
    
    # Add socket file if specified
    if config.get('MARIADB_SOCKET'):
        command.append(f"--socket={config['MARIADB_SOCKET']}")

    # Log connection details for debugging
    logger.info(f"Starting full backup to {target_dir} with connection details:")
    logger.info(f"Host: {db_host}, Port: {db_port}")
    logger.info(f"Socket: {config.get('MARIADB_SOCKET', 'Not specified')}")
    logger.info(f"User: {db_user}")
    logger.info(f"Command: {' '.join(command)}")
    print(f"Executing command: {' '.join(command)}")
    try:
        subprocess.run(command, check=True, capture_output=True)
        logger.info(f"Full backup to {target_dir} completed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Full backup to {target_dir} failed: {e}")
        if e.stdout:
            logger.error(f"Stdout: {e.stdout.decode()}")
        if e.stderr:
            logger.error(f"Stderr: {e.stderr.decode()}")
        return False
    except FileNotFoundError:
        logger.error(f"Mariabackup executable not found at {mariadb_backup_path}")
        return False

def take_incremental_backup(target_dir, basedir, config):
    """
    Executes an incremental Mariabackup.

    Args:
        target_dir (str): The directory to store the incremental backup.
        basedir (str): The base directory of the previous backup.
        config (dict): The configuration parameters.

    Returns:
        bool: True on success, False on failure.
    """
    # Log integration test status prominently
    if config.get('IS_INTEGRATION_TEST'):
        # Use print for better visibility
        print("="*50)
        print("=== RUNNING IN INTEGRATION TEST MODE ===")
        print("="*50)
        # Ensure socket file is specified for test instance
        if not config.get('MARIADB_SOCKET'):
            logger.error("MARIADB_SOCKET must be specified for integration testing")
            return False
        # Verify we're backing up the test instance
        if not verify_test_instance(config):
            logger.error("Integration test verification failed: test_db not found. Are we connected to the right instance?")
            return False
            
    mariadb_backup_path = config.get('MARIADB_BACKUP_PATH')
    db_host = config.get('DB_HOST')
    db_port = config.get('DB_PORT')
    db_user = config.get('DB_USER')
    db_password = config.get('DB_PASSWORD')

    # Build base command with common options
    command = [
        mariadb_backup_path,
        "--backup",
        f"--target-dir={target_dir}",
        f"--incremental-basedir={basedir}",
        f"--host={db_host}",
        f"--port={db_port}",
        f"--user={db_user}",
        f"--password={db_password}",
    ]
    
    # Add socket file if specified
    if config.get('MARIADB_SOCKET'):
        command.append(f"--socket={config['MARIADB_SOCKET']}")

    # Log connection details for debugging
    logger.info(f"Starting incremental backup to {target_dir} based on {basedir} with connection details:")
    logger.info(f"Host: {db_host}, Port: {db_port}")
    logger.info(f"Socket: {config.get('MARIADB_SOCKET', 'Not specified')}")
    logger.info(f"User: {db_user}")
    logger.info(f"Command: {' '.join(command)}")
    print(f"Executing command: {' '.join(command)}")
    try:
        subprocess.run(command, check=True, capture_output=True)
        logger.info(f"Incremental backup to {target_dir} completed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Incremental backup to {target_dir} failed: {e}")
        if e.stdout:
            logger.error(f"Stdout: {e.stdout.decode()}")
        if e.stderr:
            logger.error(f"Stderr: {e.stderr.decode()}")
        return False
    except FileNotFoundError:
        logger.error(f"Mariabackup executable not found at {mariadb_backup_path}")
        return False

def prepare_backup(target_dir, incremental_dirs=None, config=None):
    """
    Prepares the backup by applying logs.

    Args:
        target_dir (str): The directory of the backup to prepare.
        incremental_dirs (list, optional): List of incremental backup directories. Defaults to None.
        config (dict, optional): The configuration parameters. Defaults to None.

    Returns:
        bool: True on success, False on failure.
    """
    mariadb_backup_path = config.get('MARIADB_BACKUP_PATH')
    command = [
        mariadb_backup_path,
        "--prepare",
        f"--target-dir={target_dir}",
    ]
    if incremental_dirs:
        for inc_dir in incremental_dirs:
            command.append(f"--incremental-dir={inc_dir}")

    logger.info(f"Preparing backup in {target_dir}")
    print(f"Executing command: {' '.join(command)}")
    try:
        subprocess.run(command, check=True, capture_output=True)
        logger.info(f"Backup in {target_dir} prepared successfully.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Preparing backup in {target_dir} failed: {e}")
        if e.stdout:
            logger.error(f"Stdout: {e.stdout.decode()}")
        if e.stderr:
            logger.error(f"Stderr: {e.stderr.decode()}")
        return False
    except FileNotFoundError:
        logger.error(f"Mariabackup executable not found at {mariadb_backup_path}")
        return False

def restore_backup(backup_dir, config, method='copy-back'):
    """
    Restores the backup to the MariaDB data directory.

    Args:
        backup_dir (str): The directory of the backup to restore.
        config (dict): The configuration parameters.
        method (str, optional): The restore method ('copy-back' or 'move-back'). Defaults to 'copy-back'.

    Returns:
        bool: True on success, False on failure.
    """
    # Clean data directory before restore
    logger.info("In mariadb.restore_backup")
    datadir = config.get('MARIADB_DATADIR')
    if not datadir:
        logger.error("MARIADB_DATADIR not specified in config")
        return False
        
    # Get required directories from config
    innodb_data_dir = config.get('INNODB_DATA_HOME_DIR', datadir)  # Default to datadir if not specified
    innodb_log_dir = config.get('INNODB_LOG_GROUP_HOME_DIR', innodb_data_dir)
    logger.info("Got datadir %s %s %s", datadir, innodb_data_dir, innodb_log_dir)

    try:
        # Clean up all directories
        dirs_to_clean = [
            datadir,
            innodb_data_dir,
            innodb_log_dir
        ]
        
        # Remove all contents from directories
        for dir_path in dirs_to_clean:
            if os.path.exists(dir_path):
                logger.info(f"Cleaning directory: {dir_path}")
                try:
                    # Remove directory and recreate it
                    shutil.rmtree(dir_path)
                    os.makedirs(dir_path)
                except Exception as e:
                    logger.error(f"Failed to clean directory {dir_path}: {e}")
                    return False
                
    #     # Ensure all required directories exist with proper permissions
    #     dirs_to_create = dirs_to_clean
        
    #     # Create directories and set permissions
    #     for dir_path in dirs_to_create:
    #         try:
    #             # Create directory if it doesn't exist
    #             os.makedirs(dir_path, exist_ok=True)
                
    #             # Set directory permissions (750)
    #             os.chmod(dir_path, 0o750)
                
    #             # Set ownership to mysql:mysql
    #             shutil.chown(dir_path, 'mysql', 'mysql')
                
    #             logger.info(f"Directory prepared: {dir_path}")
    #         except Exception as e:
    #             logger.error(f"Failed to prepare directory {dir_path}: {e}")
    #             return False
                
    #     # Log directory structure for debugging
    #     logger.info("Directory structure prepared for restore:")
    #     logger.info(f"Data directory: {datadir}")
    #     logger.info(f"InnoDB data directory: {innodb_data_dir}")
    #     logger.info(f"InnoDB log directory: {innodb_log_dir}")
            
    except Exception as e:
        logger.error(f"Failed to prepare directories for restore: {e}")
        return False
    mariadb_backup_path = config.get('MARIADB_BACKUP_PATH')
    # Build base command
    command = [
        mariadb_backup_path,
    ]
    if method == 'copy-back':
        command.append("--copy-back")
    elif method == 'move-back':
        command.append("--move-back")
    else:
        logger.error(f"Invalid restore method: {method}")
        return False
        
    # Add required parameters
    # Get the latest backup directory
    
    logger.info(f"Using extracted backup directory: {backup_dir}")
    command.extend([
        f"--target-dir={backup_dir}",
        f"--datadir={datadir}"
    ])
    
    # Add InnoDB directory parameters if they differ from datadir
    if innodb_data_dir != datadir:
        command.append(f"--innodb-data-home-dir={innodb_data_dir}")
    if innodb_log_dir != datadir:
        command.append(f"--innodb-log-group-home-dir={innodb_log_dir}")
    logger.info(f"Restoring backup from {backup_dir} using {method}")
    print(f"Executing command: {' '.join(command)}")
    try:
        # Restore the backup
        subprocess.run(command, check=True, capture_output=False)
        
        # Fix ownership after restore
        restored_dirs = [
            datadir,
            innodb_data_dir,
            innodb_log_dir
        ]
        try:
            # Set ownership to mysql:mysql
            subprocess.run(['chown', '-R', 'mysql:mysql'] + restored_dirs, check=True)
            logger.info("Fixed ownership after restore")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to fix ownership after restore: {e}")
            return False
            
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Restoring backup from {backup_dir} failed: {e}")
        if e.stdout:
            logger.error(f"Stdout: {e.stdout.decode()}")
        if e.stderr:
            logger.error(f"Stderr: {e.stderr.decode()}")
        return False
    except FileNotFoundError:
        logger.error(f"Mariabackup executable not found at {mariadb_backup_path}")
        return False

if __name__ == '__main__':
    # This is just for testing purposes and won't be run when imported as a module
    class MockConfig:
        def get(self, key):
            if key == 'MARIADB_BACKUP_PATH':
                return '/usr/bin/mariabackup'
            elif key == 'DB_HOST':
                return 'localhost'
            elif key == 'DB_PORT':
                return '3306'
            elif key == 'DB_USER':
                return 'root'
            elif key == 'DB_PASSWORD':
                return 'password'
            return None

    config = MockConfig()
    temp_dir = 'temp_backup'
    import os
    os.makedirs(temp_dir, exist_ok=True)
    if take_full_backup(temp_dir, config):
        print("Full backup successful")
    else:
        print("Full backup failed")

    incremental_dir = 'temp_incremental'
    os.makedirs(incremental_dir, exist_ok=True)
    if take_incremental_backup(incremental_dir, temp_dir, config):
        print("Incremental backup successful")
    else:
        print("Incremental backup failed")

    if prepare_backup(temp_dir, config=config):
        print("Prepare backup successful")
    else:
        print("Prepare backup failed")

    if restore_backup(temp_dir, config):
        print("Restore backup successful")
    else:
        print("Restore backup failed")
