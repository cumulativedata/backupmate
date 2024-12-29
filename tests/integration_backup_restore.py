import unittest
import os
import sys
import logging
import subprocess
from datetime import datetime
from backupmate import cli, config, s3

# claude, don't run this test file automatically. this must be run manually in remote.

class BackupRestoreIntegrationTest(unittest.TestCase):
    """Integration test for complete backup and restore cycle using CLI interface."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        
        # Load and validate config from .backupmate.env
        cls.config = config.load_config()
        config.validate_config(cls.config)
        
        # Store original sys.argv
        cls.original_argv = sys.argv

    def setUp(self):
        """Set up test case."""
        # Create unique timestamp for this test run
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
    def tearDown(self):
        """Clean up after test."""
        # Restore original sys.argv
        sys.argv = self.original_argv

    def check_creds_perms(self):
        """
        Verify all credentials and permissions needed for backup operations.
        Returns a dictionary of test results with details about each check.
        """
        print("\n=== MariaDB Backup Permission Check ===")
        print("This test verifies all permissions needed for mariabackup to work correctly")
        
        results = {
            'database': {'status': True, 'details': []},
            'system': {'status': True, 'details': []},
            's3': {'status': True, 'details': []}
        }
        
        # 1. Database Checks
        print("\n[1/3] Database Permissions")
        try:
            print("\nTesting MariaDB connection...")
            print(f"- Connecting to: {self.config['DB_HOST']}:{self.config['DB_PORT']}")
            print(f"- Using user: {self.config['DB_USER']}")
            cmd = [
                'mysql',
                f'-h{self.config["DB_HOST"]}',
                f'-P{self.config["DB_PORT"]}',
                f'-u{self.config["DB_USER"]}',
                f'-p{self.config["DB_PASSWORD"]}',
                '-e', 'SELECT 1;'
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                results['database']['status'] = False
                results['database']['details'].append(f"Connection failed: {result.stderr}")
            
            print("\nChecking MariaDB privileges...")
            print("Required privileges for mariabackup:")
            print("- RELOAD (for flushing tables)")
            print("- LOCK TABLES (for consistent backup)")
            print("Optional privileges:")
            print("- REPLICATION CLIENT (only if binary log position tracking is needed)")
            privileges_query = """
            SELECT PRIVILEGE_TYPE 
            FROM information_schema.USER_PRIVILEGES 
            WHERE GRANTEE = CONCAT("'", REPLACE(CURRENT_USER(), '@', "'@'"), "'");
            """
            cmd[-1] = privileges_query
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            # REPLICATION CLIENT is only needed if binary log position is required
            required_privileges = {'RELOAD', 'LOCK TABLES'}
            if result.returncode == 0:
                granted_privileges = {priv.strip() for priv in result.stdout.split('\n') if priv.strip()}
                print("\nGranted privileges:", granted_privileges)
                missing_privileges = required_privileges - granted_privileges
                if missing_privileges:
                    results['database']['status'] = False
                    results['database']['details'].append(f"Missing privileges: {missing_privileges}")
            else:
                results['database']['status'] = False
                results['database']['details'].append(f"Failed to check privileges: {result.stderr}")
                
        except Exception as e:
            results['database']['status'] = False
            results['database']['details'].append(f"Database check error: {str(e)}")
            
        # 2. System Checks
        print("\n[2/3] System Permissions")
        try:
            print("\nChecking if running as sudo...")
            if os.geteuid() != 0:
                results['system']['status'] = False
                results['system']['details'].append("This test must be run with sudo")
                
            print("\nChecking mariabackup installation...")
            mariabackup_path = self.config.get('MARIADB_BACKUP_PATH')
            print(f"- Looking for mariabackup at: {mariabackup_path}")
            if not os.path.exists(mariabackup_path):
                results['system']['status'] = False
                results['system']['details'].append(f"Mariabackup not found at {mariabackup_path}")
            
            print("\nChecking backup directory permissions...")
            test_backup_dir = '/tmp/backupmate_test'
            print(f"- Testing write access to: {test_backup_dir}")
            try:
                os.makedirs(test_backup_dir, exist_ok=True)
                test_file = os.path.join(test_backup_dir, 'test_write')
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
            except PermissionError as e:
                results['system']['status'] = False
                results['system']['details'].append(f"Backup directory permission error: {str(e)}")
            finally:
                try:
                    os.rmdir(test_backup_dir)
                except:
                    pass
                    
        except Exception as e:
            results['system']['status'] = False
            results['system']['details'].append(f"System check error: {str(e)}")
            
        # 3. S3 Checks
        print("\n[3/3] S3 Storage Access")
        try:
            print("\nVerifying S3 credentials...")
            print(f"- Bucket: {self.config['S3_BUCKET_NAME']}")
            print(f"- Region: {self.config.get('AWS_REGION', 'default')}")
            print(f"- Backup prefix: {self.config['FULL_BACKUP_PREFIX']}")
            test_objects = s3.list_objects(
                self.config['S3_BUCKET_NAME'],
                self.config['FULL_BACKUP_PREFIX'],
                self.config
            )
            
            print("\nTesting S3 permissions...")
            print("- Checking LIST permission")
            test_key = f"{self.config['FULL_BACKUP_PREFIX']}/test_permissions"
            print("- Checking WRITE permission (uploading test file)")
            try:
                s3.upload_file(
                    'README.md',  # Using existing file for test
                    self.config['S3_BUCKET_NAME'],
                    test_key,
                    self.config
                )
                print("- Checking DELETE permission (cleaning up test file)")
                s3.delete_object(
                    self.config['S3_BUCKET_NAME'],
                    test_key,
                    self.config
                )
            except Exception as e:
                results['s3']['status'] = False
                results['s3']['details'].append(f"S3 write/delete test failed: {str(e)}")
                
        except Exception as e:
            results['s3']['status'] = False
            results['s3']['details'].append(f"S3 check error: {str(e)}")
            
        # Print Summary
        print("\n=== Permission Check Summary ===")
        all_passed = True
        for category, result in results.items():
            status = "✓ PASS" if result['status'] else "✗ FAIL"
            print(f"\n{category.upper()}: {status}")
            if not result['status']:
                all_passed = False
                for detail in result['details']:
                    print(f"  - {detail}")
        
        if all_passed:
            print("\n✓ All permission checks passed - backup operations should work correctly")
        else:
            print("\n✗ Some checks failed - please fix the issues above before running backups")
        
        print("\n=== End of Permission Check ===")
        return results

    def verify_database_state(self):
        """Verify database is accessible and running."""
        try:
            # Try to connect to database using mysql client
            cmd = [
                'mysql',
                f'-h{self.config["DB_HOST"]}',
                f'-P{self.config["DB_PORT"]}',
                f'-u{self.config["DB_USER"]}',
                f'-p{self.config["DB_PASSWORD"]}',
                '-e', 'SELECT 1;'
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            logging.error(f"Failed to verify database state: {e}")
            return False

    def test_backup_restore_cycle(self):
        """Test complete backup and restore cycle through CLI interface."""
        # Step 1: Perform full backup
        sys.argv = ['backupmate', 'backup', '--full']
        result = cli.main()
        self.assertEqual(result, 0, "Full backup command failed")
        
        # Step 2: List and verify backup exists in S3
        sys.argv = ['backupmate', 'list', '--json']
        result = cli.main()
        self.assertEqual(result, 0, "List command failed")
        
        # Verify backup files exist in S3
        objects = s3.list_objects(
            self.config['S3_BUCKET_NAME'],
            self.config['FULL_BACKUP_PREFIX'],
            self.config
        )
        self.assertTrue(len(objects) > 0, "No backup files found in S3")
        
        # Get the latest backup ID
        latest_backup = sorted(objects)[-1]
        
        # Step 3: Restore the backup
        sys.argv = ['backupmate', 'restore', latest_backup, '--copy-back']
        result = cli.main()
        self.assertEqual(result, 0, "Restore command failed")
        
        # Step 4: Verify database is accessible after restore
        self.assertTrue(
            self.verify_database_state(),
            "Database is not accessible after restore"
        )

    def test_check_creds_perms(self):
        """Test all credentials and permissions needed for backup operations."""
        results = self.check_creds_perms()
        
        # Build comprehensive error message
        error_messages = []
        for category, result in results.items():
            if not result['status']:
                error_messages.append(f"\n{category.upper()} ISSUES:")
                error_messages.extend([f"  - {detail}" for detail in result['details']])
        
        # Assert and provide detailed feedback
        all_passed = all(r['status'] for r in results.values())
        self.assertTrue(all_passed, "\nPermission/Credential checks failed:" + "".join(error_messages))

if __name__ == '__main__':
    unittest.main()
