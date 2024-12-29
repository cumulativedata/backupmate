import argparse
import json
import os
import sys
from typing import Optional, Dict, Any
from pathlib import Path

from backupmate.config import load_config, validate_config
from backupmate.logger import setup_logger, log_info, log_error
from backupmate.backup import perform_full_backup, perform_incremental_backup
from backupmate.restore import restore_specific_backup
from backupmate.s3 import list_objects
from backupmate.utils import ensure_directory

def handle_backup(args: argparse.Namespace, config: Dict[str, Any], logger: Any) -> bool:
    """Handle backup command."""
    try:
        # Ensure temp directory exists
        if not ensure_directory(config['LOCAL_TEMP_DIR']):
            log_error(logger, "Failed to create temporary directory")
            return False

        if args.full:
            log_info(logger, "Starting full backup")
            success = perform_full_backup(config)
        else:
            log_info(logger, "Starting incremental backup")
            success = perform_incremental_backup(config)

        if success:
            log_info(logger, "Backup completed successfully")
        else:
            log_error(logger, "Backup failed")
        return success
    except Exception as e:
        log_error(logger, f"Error during backup: {str(e)}")
        return False

def handle_restore(args: argparse.Namespace, config: Dict[str, Any], logger: Any) -> bool:
    """Handle restore command."""
    try:
        if sum([bool(args.backup_id), args.latest_full, args.latest_incremental]) != 1:
            log_error(logger, "Must specify exactly one of: backup_id, --latest-full, or --latest-incremental")
            return False

        # Determine restore method
        if args.move_back and args.copy_back:
            log_error(logger, "Cannot specify both --move-back and --copy-back")
            return False
        restore_method = 'move-back' if args.move_back else 'copy-back'

        log_info(logger, "Starting restore operation")
        # Determine backup identifier based on flags
        backup_identifier = args.backup_id
        if args.latest_full:
            # TODO: Get latest full backup identifier from S3
            backup_identifier = list_objects(
                config['S3_BUCKET_NAME'],
                config['FULL_BACKUP_PREFIX'],
                config
            )[-1] if list_objects(config['S3_BUCKET_NAME'], config['FULL_BACKUP_PREFIX'], config) else None
        elif args.latest_incremental:
            # TODO: Get latest incremental backup identifier from S3
            backup_identifier = list_objects(
                config['S3_BUCKET_NAME'],
                config['INCREMENTAL_BACKUP_PREFIX'],
                config
            )[-1] if list_objects(config['S3_BUCKET_NAME'], config['INCREMENTAL_BACKUP_PREFIX'], config) else None

        if not backup_identifier:
            log_error(logger, "No backup found to restore")
            return False

        success = restore_specific_backup(
            backup_identifier=backup_identifier,
            restore_method=restore_method,
            config=config
        )

        if success:
            log_info(logger, "Restore completed successfully")
        else:
            log_error(logger, "Restore failed")
        return success
    except Exception as e:
        log_error(logger, f"Error during restore: {str(e)}")
        return False

def handle_list(args: argparse.Namespace, config: Dict[str, Any], logger: Any) -> bool:
    """Handle list command."""
    try:
        log_info(logger, "Listing available backups")
        
        # List full backups
        full_backups = list_objects(
            config['S3_BUCKET_NAME'],
            config['FULL_BACKUP_PREFIX'],
            config
        )
        
        # List incremental backups
        incremental_backups = list_objects(
            config['S3_BUCKET_NAME'],
            config['INCREMENTAL_BACKUP_PREFIX'],
            config
        )
        
        output = {
            'full_backups': full_backups,
            'incremental_backups': incremental_backups
        }
        
        if args.json:
            print(json.dumps(output, indent=2))
        else:
            print("\nFull Backups:")
            for backup in full_backups:
                print(f"  - {backup}")
            print("\nIncremental Backups:")
            for backup in incremental_backups:
                print(f"  - {backup}")
                
        return True
    except Exception as e:
        log_error(logger, f"Error listing backups: {str(e)}")
        return False

def main() -> Optional[int]:
    """Main entry point for the BackupMate CLI."""
    parser = argparse.ArgumentParser(description="Backup and restore your MariaDB database.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Backup command
    backup_parser = subparsers.add_parser("backup", help="Perform a backup")
    backup_parser.add_argument("--full", action="store_true", help="Perform a full backup")

    # Restore command
    restore_parser = subparsers.add_parser("restore", help="Restore a backup")
    restore_parser.add_argument("backup_id", nargs="?", help="ID of the backup to restore")
    restore_parser.add_argument("--latest-full", action="store_true", help="Restore the latest full backup")
    restore_parser.add_argument("--latest-incremental", action="store_true", help="Restore the latest incremental backup")
    restore_parser.add_argument("--copy-back", action="store_true", help="Copy back backup files (default)")
    restore_parser.add_argument("--move-back", action="store_true", help="Move back backup files")
    restore_parser.add_argument("--json", action="store_true", help="Output in JSON format")

    # List backups command
    list_parser = subparsers.add_parser("list", help="List available backups")
    list_parser.add_argument("--json", action="store_true", help="Output in JSON format")

    args = parser.parse_args()

    try:
        # Check if running as root/sudo for backup and restore commands
        if args.command in ["backup", "restore"] and os.geteuid() != 0:
            print("Error: backup and restore commands must be run with sudo", file=sys.stderr)
            return 1

        # Load and validate configuration
        config = load_config()
        if not validate_config(config):
            print("Error: Invalid configuration", file=sys.stderr)
            return 1

        # Setup logging
        logger = setup_logger("backupmate")

        if args.command == "backup":
            success = handle_backup(args, config, logger)
        elif args.command == "restore":
            success = handle_restore(args, config, logger)
        elif args.command == "list":
            success = handle_list(args, config, logger)
        else:
            parser.print_help()
            return 0

        return 0 if success else 1

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
