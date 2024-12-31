# BackupMate

A command-line tool for backing up and restoring MariaDB databases to AWS S3.

## Features

*   Full and incremental backups using Mariabackup.
*   Storage on AWS S3.
*   Configuration via `.backupmate.env` file.
*   Backup metadata stored in an SQLite database.
*   CLI interface for backups and restores.
*   JSON output for AI integration.

## Installation

```bash
pip install .
```

## Configuration

Create a `.backupmate.env` file in the directory where you run the `backupmate` command.

### Required Configuration

```env
# Database Connection
DB_HOST=localhost                    # Database host
DB_PORT=3306                        # Database port (must be integer)
DB_USER=backup_user                 # Database user with backup privileges
DB_PASSWORD=secure_password         # Database password
MARIADB_SOCKET=/var/run/mysqld/mysqld.sock  # MariaDB socket file path

# Backup Tool
MARIADB_BACKUP_PATH=/usr/bin/mariabackup  # Path to mariabackup executable (absolute path)
LOCAL_TEMP_DIR=/tmp/backupmate      # Temporary directory for backups (absolute path)

# AWS S3 Configuration
S3_BUCKET_NAME=your-s3-bucket-name  # S3 bucket for storing backups
AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY   # AWS access key
AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY  # AWS secret key
AWS_REGION=us-east-1               # AWS region

# Backup Strategy
FULL_BACKUP_PREFIX=backupmate/full/  # S3 prefix for full backups (must end with /)
INCREMENTAL_BACKUP_PREFIX=backupmate/incremental/  # S3 prefix for incremental backups (must end with /)
FULL_BACKUP_SCHEDULE=weekly         # Schedule for full backups ('weekly' or 'monthly')
```

### Optional Configuration

```env
# Database Server Management
MARIADB_DATADIR=/var/lib/mysql     # Custom data directory path
MYSQL_START_COMMAND=systemctl start mariadb  # Custom command to start MariaDB
MYSQL_STOP_COMMAND=systemctl stop mariadb   # Custom command to stop MariaDB

# Metadata Storage
SQLITE_FILE=backupmate.db                # SQLite database file path (absolute or relative to working directory)
```

### Configuration Validation Rules

- All paths (MARIADB_BACKUP_PATH, LOCAL_TEMP_DIR, MARIADB_SOCKET, MARIADB_DATADIR) must be absolute
- DB_PORT must be an integer
- FULL_BACKUP_SCHEDULE must be either 'weekly' or 'monthly'
- S3 prefix paths must end with '/'

## Usage

```bash
backupmate --help
```

### Automated Backups

BackupMate can be configured to run automated backups via cron. It automatically determines whether to perform a full or incremental backup based on the schedule and existing backups.

Example cron configuration:
```bash
# Daily incremental backup at 1 AM
0 1 * * * sudo backupmate backup

# Weekly full backup on Sundays at 2 AM
0 2 * * 0 sudo backupmate backup --full
```

### Manual Backups

```bash
# Perform an incremental backup
sudo backupmate backup

# Force a full backup
sudo backupmate backup --full
```

Note: Backup commands require sudo privileges on Unix systems.

### Restoring Backups

```bash
# Restore a specific backup by ID
sudo backupmate restore <backup_id>

# Restore the latest full backup
sudo backupmate restore --latest-full

# Restore the latest incremental backup
sudo backupmate restore --latest-incremental

# Restore using move-back instead of copy-back
sudo backupmate restore <backup_id> --move-back

# Get restore output in JSON format
sudo backupmate restore --latest-full --json
```

The restore process:
1. Downloads necessary backup files from S3
2. Prepares the backup (applies incremental changes if needed)
3. Optionally stops the MariaDB server
4. Restores files using either copy-back or move-back method
5. Sets appropriate file permissions
6. Optionally starts the MariaDB server

### Listing Backups

```bash
# List available backups in human-readable format
backupmate list

# List backups in JSON format
backupmate list --json
```

## Development

### Setting up the development environment

1. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Unix
venv\Scripts\activate     # On Windows
```

2. Install in development mode:
```bash
pip install -e .
```

### Running Tests

#### Unit Tests
Unit tests are safe to run as they use mocking for all external operations (database, S3, file system):
```bash
# Run all unit tests with verbose output
~/venv_backupmate/bin/python -m unittest discover -s tests -v
```

#### Integration Tests
Integration tests require sudo privileges as they create a test MariaDB instance:
```bash
# Run integration test suite
sudo ~/venv_backupmate/bin/python -m unittest tests.integration_backup_restore.BackupRestoreIntegrationTest
```

The integration test performs a complete backup and restore cycle:
1. Sets up a test MariaDB instance on port 3307
2. Creates test databases and tables with sample data
3. Performs a full backup to S3
4. Simulates data loss by dropping the test database
5. Restores from the backup
6. Verifies the restored data matches the original

This ensures the entire backup/restore workflow functions correctly in a real environment.

### Error Handling and Logging

BackupMate implements comprehensive error handling and logging:

- All operations are logged in JSON format
- Logs include timestamps, actions performed, and any errors
- Failed S3 operations are automatically retried
- Detailed error messages help diagnose issues
- Logs are stored locally for troubleshooting

### Project Structure

```
backupmate/
├── backupmate/           # Main package
│   ├── cli.py           # CLI interface
│   ├── config.py        # Configuration management
│   ├── mariadb.py       # MariaDB operations
│   ├── s3.py            # S3 operations
│   ├── backup.py        # Backup logic
│   ├── restore.py       # Restore logic
│   ├── logger.py        # JSON logging
│   └── utils.py         # Utilities
└── tests/               # Test suite
    ├── test_*.py        # Unit tests
    └── integration_*.py # Integration tests
```
