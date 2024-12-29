Design for Mariabackup Wrapper Script: "BackupMate"
This design outlines a Python CLI tool named "BackupMate" for automating full and incremental MariaDB backups using Mariabackup and storing them on AWS S3. It also allows for manual restoration of specific backups.

Concrete Requirements:

Configuration:

The script should load configuration parameters from a .backupmate.env file located in the current working directory.

The .backupmate.env file should store:

DB_HOST: Database host.

DB_PORT: Database port.

DB_USER: Database user.

DB_PASSWORD: Database password.

MARIADB_BACKUP_PATH: Path to the Mariabackup executable.

S3_BUCKET_NAME: S3 bucket name.

AWS_ACCESS_KEY_ID: AWS access key ID.

AWS_SECRET_ACCESS_KEY: AWS secret access key.

AWS_REGION: AWS region.

LOCAL_TEMP_DIR: Local temporary directory for backups.

FULL_BACKUP_PREFIX: Prefix for full backups in S3 (e.g., backupmate/full/).

INCREMENTAL_BACKUP_PREFIX: Prefix for incremental backups in S3 (e.g., backupmate/incremental/).

FULL_BACKUP_SCHEDULE: Backup schedule for full backups (e.g., weekly, monthly).

Automated Backups (Cron):

The script should be executable via cron.

It should determine whether to perform a full or incremental backup based on the schedule and the existence of previous backups in S3.

Full backups should be taken periodically as defined in the configuration.

Incremental backups should be taken more frequently (e.g., daily) and based on the latest successful full backup identified in S3.

Each backup (full and incremental) should be stored in a uniquely named directory locally.

After a successful backup, the local backup directory should be compressed (e.g., using tar.gz) and uploaded to the appropriate S3 prefix.

The script should log all actions in JSON format, including start/end times, success/failure status, and any errors.

Manual Restore:

The script should accept command-line arguments to initiate a restore operation.

Users should be able to specify the backup to restore, either by:

A specific full backup timestamp.

The latest full backup.

A specific incremental backup timestamp (which will require restoring the corresponding full backup and all preceding incrementals).

The script should download the necessary backup files (full and incrementals) from S3 to the local temporary directory.

The script should then use Mariabackup to prepare the backups, applying incremental changes to the base full backup.

The script should provide options for restoring:

--copy-back: Copies the restored files to the MariaDB data directory.

--move-back: Moves the restored files to the MariaDB data directory.

The script should handle stopping and starting the MariaDB server during the restore process (with appropriate confirmation prompts or configuration options).

The script should handle file permissions after restoration.

Error Handling:

The script should implement robust error handling for all critical operations (Mariabackup execution, S3 interaction, file operations).

Errors should be logged with detailed information in JSON format.

For automated backups, the script should attempt to retry failed operations (e.g., S3 upload) a configurable number of times.

The script should send notifications (e.g., email or Slack) on backup failures (optional but recommended).

Logging:

The script should maintain detailed logs of all operations in JSON format, including timestamps, actions performed, and any errors encountered.

Logs should be stored locally.

Unit Tests:

Each module/function should have corresponding unit tests using the `unittest` framework to ensure functionality and prevent regressions.

Project Folder Structure:

backupmate/
+-- backupmate/
�   +-- __init__.py
�   +-- cli.py          # Entry point for the CLI
�   +-- config.py       # Handles configuration loading
�   +-- mariadb.py      # Handles Mariabackup interactions
�   +-- s3.py           # Handles S3 interactions
�   +-- backup.py       # Handles backup logic (full/incremental scheduling)
�   +-- restore.py      # Handles restore logic
�   +-- logger.py       # Handles JSON logging
�   +-- utils.py        # Utility functions
+-- tests/
�   +-- __init__.py
�   +-- test_config.py
�   +-- test_mariadb.py
�   +-- test_s3.py
�   +-- test_backup.py
�   +-- test_restore.py
�   +-- test_logger.py
�   +-- test_utils.py
+-- .backupmate.env     # Example environment file
+-- pyproject.toml      # For packaging and dependencies
+-- README.md
+-- LICENSE
Use code with caution.
File and Function Descriptions:

1. backupmate/config.py

Test File: tests/test_config.py

Functions:

load_config(): Loads configuration parameters from the .backupmate.env file using a library like dotenv. Returns a dictionary or object containing the configuration.

validate_config(config): Validates the loaded configuration to ensure all required parameters are present and of the correct type. Raises an exception if validation fails.

2. backupmate/mariadb.py

Test File: tests/test_mariadb.py

Functions:

take_full_backup(target_dir, config): Executes a full Mariabackup using the provided configuration. Returns True on success, False on failure.

take_incremental_backup(target_dir, basedir, config): Executes an incremental Mariabackup. Returns True on success, False on failure.

prepare_backup(target_dir, incremental_dirs=None, config): Prepares the backup by applying logs. Returns True on success, False on failure.

restore_backup(backup_dir, config, method='copy-back'): Restores the backup to the MariaDB data directory. Returns True on success, False on failure.

3. backupmate/s3.py

Test File: tests/test_s3.py

Functions:

upload_directory(local_path, s3_bucket, s3_prefix, config): Uploads a local directory to S3. Returns True on success, False on failure.

download_directory(s3_bucket, s3_prefix, local_path, config): Downloads a directory from S3. Returns True on success, False on failure.

list_objects(s3_bucket, prefix, config): Lists objects in an S3 bucket with a given prefix. Returns a list of object keys.

get_latest_backup_prefix(s3_bucket, prefix, config): Retrieves the prefix of the latest backup (full or incremental) based on timestamp.

4. backupmate/backup.py

Test File: tests/test_backup.py

Functions:

perform_full_backup(config): Orchestrates the full backup process (Mariabackup, compression, S3 upload). Returns True on success, False on failure.

perform_incremental_backup(config, base_prefix): Orchestrates the incremental backup process. Returns True on success, False on failure.

get_latest_full_backup_prefix(s3_bucket, full_backup_prefix, config): Retrieves the S3 prefix of the latest successful full backup.

5. backupmate/restore.py

Test File: tests/test_restore.py

Functions:

restore_specific_backup(backup_identifier, restore_method, config): Orchestrates the restore process for a specified backup (downloads from S3, prepares, restores). Returns True on success, False on failure.

download_and_prepare_backup(backup_prefix, local_staging_dir, config): Downloads the necessary backup files from S3 and prepares them for restoration. Returns True on success, False on failure.

stop_mariadb_server(): Stops the MariaDB server (implementation depends on OS).

start_mariadb_server(): Starts the MariaDB server (implementation depends on OS).

6. backupmate/logger.py

Test File: tests/test_logger.py

Functions:

setup_logger(name): Sets up and returns a logger instance that outputs logs in JSON format.

log_info(logger, message, data=None): Logs an informational message with optional additional data.

log_error(logger, message, data=None): Logs an error message with optional additional data.

7. backupmate/utils.py

Test File: tests/test_utils.py

Functions:

compress_directory(dir_path, output_path): Compresses a directory into a tar.gz archive.

decompress_archive(archive_path, output_path): Decompresses a tar.gz archive.

8. backupmate/cli.py

Test File: (Integration tests might be more suitable for testing the CLI)

Functions:

main(): The main entry point for the CLI application.

Uses argparse to handle command-line arguments for backup and restore operations.

Calls the appropriate functions from other modules based on the user's input.

Unit Test Functions:

Each test file in the tests/ directory will contain unit tests for the corresponding module. Here are examples of the types of tests that would be included:

tests/test_config.py:

test_load_config(): Tests if the configuration is loaded correctly from the .env file.

test_validate_config_success(): Tests if valid configuration passes validation.

test_validate_config_failure(): Tests if invalid configuration raises an exception.

tests/test_mariadb.py:

test_take_full_backup_success(): Tests if a full backup command is executed successfully (mocking the external mariabackup call).

test_take_full_backup_failure(): Tests if the function handles errors during full backup execution.

Similar tests for take_incremental_backup, prepare_backup, and restore_backup.

tests/test_s3.py:

test_upload_directory_success(): Tests if a directory is uploaded to S3 successfully (mocking the boto3 S3 client).

test_upload_directory_failure(): Tests error handling during S3 upload.

Similar tests for download_directory, list_objects, and get_latest_backup_prefix.

tests/test_backup.py:

test_perform_full_backup_success(): Tests the orchestration of the full backup process.

test_perform_incremental_backup_success(): Tests the orchestration of the incremental backup process.

test_get_latest_full_backup_prefix(): Tests the retrieval of the latest full backup prefix from S3.

tests/test_restore.py:

test_restore_specific_backup_success(): Tests the orchestration of the restore process.

test_download_and_prepare_backup_success(): Tests downloading and preparing backups.

tests/test_logger.py:

test_log_info(): Tests if informational messages are logged correctly in JSON format.

test_log_error(): Tests if error messages are logged correctly in JSON format.

tests/test_utils.py:

test_compress_directory(): Tests directory compression.

test_decompress_archive(): Tests archive decompression.

Workflow for Automated Backups (Cron):

Initialization: The script (via cli.py) loads configuration from the .backupmate.env file using config.load_config(). A logger is initialized using logger.setup_logger().

Determine Backup Type: backup.py checks S3 for the latest full backup prefix using s3.get_latest_backup_prefix(). Based on the configured schedule and the existence of a recent full backup, it determines whether to perform a full or incremental backup.

Full Backup:

Create a new local temporary directory.

Call mariadb.take_full_backup().

Compress the local backup directory using utils.compress_directory().

Upload the compressed backup to S3 using s3.upload_directory() with the FULL_BACKUP_PREFIX and a timestamped sub-prefix.

Incremental Backup:

Retrieve the latest full backup prefix using backup.get_latest_full_backup_prefix().

Create a new local temporary directory.

Call mariadb.take_incremental_backup(), using the downloaded latest full backup as the base (or the latest incremental).

Compress the local backup directory using utils.compress_directory().

Upload the compressed backup to S3 using s3.upload_directory() with the INCREMENTAL_BACKUP_PREFIX and a timestamped sub-prefix.

Logging: All actions and their status are logged in JSON format using the functions in logger.py.

Workflow for Manual Restore:

Argument Parsing: cli.py parses command-line arguments to determine the backup to restore and the restore method.

Identify Backup Files: restore.py uses the provided identifier and s3.list_objects() to identify the necessary backup files (full and incrementals) from S3.

Download Backups: restore.py downloads the identified backup files from S3 to a local staging directory using s3.download_directory().

Prepare Backup: restore.py calls mariadb.prepare_backup() to apply incremental logs to the base full backup.

Stop MariaDB Server (Optional): restore.py can optionally stop the MariaDB server.

Restore: restore.py calls mariadb.restore_backup() with the specified restore method.

Set Permissions: OS-specific commands are used to adjust file permissions.

Start MariaDB Server (Optional): restore.py can optionally start the MariaDB server.

Logging: All actions are logged in JSON format.

This updated design provides a more detailed and structured approach to building the "BackupMate" CLI tool, incorporating the requested changes and emphasizing modularity and testability.
