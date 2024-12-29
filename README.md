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

Create a `.backupmate.env` file in the directory where you run the `backupmate` command. Example:

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=backup_user
DB_PASSWORD=secure_password
MARIADB_BACKUP_PATH=/usr/bin/mariabackup
S3_BUCKET_NAME=your-s3-bucket-name
AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
AWS_REGION=us-east-1
LOCAL_TEMP_DIR=/tmp/backupmate
FULL_BACKUP_PREFIX=backupmate/full/
INCREMENTAL_BACKUP_PREFIX=backupmate/incremental/
FULL_BACKUP_SCHEDULE=weekly
```

## Usage

```bash
backupmate --help
```

### Backing up

```bash
backupmate backup
backupmate backup --full
```

### Restoring

```bash
backupmate restore <backup_id>
backupmate restore --latest-full
backupmate restore --latest-incremental
backupmate restore <backup_id> --move-back
backupmate restore --latest-full --json
```

### Listing Backups

```bash
backupmate list
backupmate list --json
```

## Development

### Setting up the development environment

```bash
python -m venv venv
source venv/bin/activate
pip install -e .
pip install -r requirements-dev.txt # Create a requirements-dev.txt for testing deps
```

### Running tests

```bash
python -m unittest discover -s tests
```
