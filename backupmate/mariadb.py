import subprocess
import logging

logger = logging.getLogger(__name__)

def take_full_backup(target_dir, config):
    """
    Executes a full Mariabackup using the provided configuration.

    Args:
        target_dir (str): The directory to store the backup.
        config (dict): The configuration parameters.

    Returns:
        bool: True on success, False on failure.
    """
    mariadb_backup_path = config.get('MARIADB_BACKUP_PATH')
    db_host = config.get('DB_HOST')
    db_port = config.get('DB_PORT')
    db_user = config.get('DB_USER')
    db_password = config.get('DB_PASSWORD')

    command = [
        mariadb_backup_path,
        "--backup",
        f"--target-dir={target_dir}",
        f"--host={db_host}",
        f"--port={db_port}",
        f"--user={db_user}",
        f"--password={db_password}",
    ]

    logger.info(f"Starting full backup to {target_dir}")
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
    mariadb_backup_path = config.get('MARIADB_BACKUP_PATH')
    db_host = config.get('DB_HOST')
    db_port = config.get('DB_PORT')
    db_user = config.get('DB_USER')
    db_password = config.get('DB_PASSWORD')

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

    logger.info(f"Starting incremental backup to {target_dir} based on {basedir}")
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
    mariadb_backup_path = config.get('MARIADB_BACKUP_PATH')
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
    command.append(f"--target-dir={backup_dir}")
    logger.info(f"Restoring backup from {backup_dir} using {method}")
    try:
        subprocess.run(command, check=True, capture_output=True)
        logger.info(f"Backup from {backup_dir} restored successfully.")
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
