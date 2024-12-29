import unittest
import os
import sys
import logging
import subprocess
import time
import shutil
from datetime import datetime
from backupmate import cli, config, s3

# claude, don't run this test file automatically. this must be run manually in remote.

class BackupRestoreIntegrationTest(unittest.TestCase):
    """Integration test for complete backup and restore cycle using CLI interface."""
    
    TEST_PORT = 3307  # Different port for test instance
    TEST_DATADIR = '/tmp/backupmate_test_datadir'
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        
        # Load config from .backupmate.env and modify for test instance
        cls.config = config.load_config()
        cls.original_config = cls.config.copy()
        
        # Update config for test instance
        cls.config.update({
            'DB_PORT': str(cls.TEST_PORT),  # Convert port to string
            'MARIADB_SOCKET': '/tmp/mariadb_backupmate_test.sock',  # Custom socket file
            'MARIADB_DATADIR': cls.TEST_DATADIR,  # Custom data directory
            'LOCAL_TEMP_DIR': '/tmp/backupmate_test_backup'  # Separate backup directory
        })
        config.validate_config(cls.config)
        
        # Store original sys.argv
        cls.original_argv = sys.argv
        
        # Start test MariaDB instance
        cls.setup_test_mariadb_instance()

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.stop_test_mariadb_instance()
        cls.cleanup_test_files()

    def setUp(self):
        """Set up test case."""
        # Create unique timestamp for this test run
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # Create test data
        self.create_test_data()
        
    def tearDown(self):
        """Clean up after test."""
        # Restore original sys.argv
        sys.argv = self.original_argv
        # Clean up test data
        self.cleanup_test_data()

    @classmethod
    def cleanup_test_files(cls):
        """Clean up any existing test files."""
        test_files = [
            cls.TEST_DATADIR,
            '/tmp/mariadb_backupmate_test.sock',
            '/tmp/mariadb_backupmate_test.pid',
            '/tmp/mariadb_backupmate_test.log',
            '/tmp/mariadb_backupmate_test_tmp',
            '/tmp/mariadb_backupmate_test_innodb',
            cls.config.get('LOCAL_TEMP_DIR', '/tmp/backupmate_test_backup')
        ]
        for file in test_files:
            try:
                if os.path.isdir(file):
                    shutil.rmtree(file)
                elif os.path.exists(file):
                    os.remove(file)
            except Exception as e:
                logging.warning(f"Failed to remove {file}: {e}")

    @classmethod
    def setup_test_mariadb_instance(cls):
        """Start a new MariaDB instance for testing."""
        # Clean up any existing files first
        cls.cleanup_test_files()
        
        # Ensure mysql user exists
        try:
            subprocess.run(['id', 'mysql'], check=True)
        except subprocess.CalledProcessError:
            logging.error("MySQL user does not exist. Please create it with: useradd mysql")
            raise
            
        # Create required directories with proper permissions
        dirs = [
            cls.TEST_DATADIR,
            '/tmp/mariadb_backupmate_test_tmp',
            '/tmp/mariadb_backupmate_test_innodb'
        ]
        for dir_path in dirs:
            logging.info(f"Creating directory: {dir_path}")
            try:
                os.makedirs(dir_path, exist_ok=True)
                logging.info(f"Directory created: {dir_path}")
                
                os.chmod(dir_path, 0o750)  # Ensure proper permissions
                logging.info(f"Permissions set to 750 for: {dir_path}")
                
                # First ensure current user owns the directory
                os.chown(dir_path, os.getuid(), os.getgid())
                logging.info(f"Changed ownership to current user for: {dir_path}")
                
                # Then set mysql user as owner
                subprocess.run(['chown', '-R', 'mysql:mysql', dir_path], check=True)
                logging.info(f"Changed ownership to mysql:mysql for: {dir_path}")
            except Exception as e:
                logging.error(f"Failed to setup directory {dir_path}: {str(e)}")
                raise
        
        # Initialize MariaDB data directory with explicit permissions
        init_cmd = [
            'mariadb-install-db',
            f'--datadir={cls.TEST_DATADIR}',
            '--user=mysql',
            '--skip-test-db',
            '--auth-root-authentication-method=normal'
        ]
        try:
            result = subprocess.run(init_cmd, capture_output=True, text=True, check=True)
            logging.info("MariaDB data directory initialized successfully")
            logging.info(f"Initialization output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to initialize MariaDB data directory: {e}")
            logging.error(f"Command output: {e.stdout}")
            logging.error(f"Command error: {e.stderr}")
            raise
            
        # Ensure mysql user has full permissions on data directory
        subprocess.run(['chmod', '-R', '750', cls.TEST_DATADIR], check=True)
        subprocess.run(['chown', '-R', 'mysql:mysql', cls.TEST_DATADIR], check=True)
        
        # Start MariaDB server with additional permissions
        server_cmd = [
            'mariadbd',
            f'--datadir={cls.TEST_DATADIR}',
            f'--port={cls.TEST_PORT}',
            '--socket=/tmp/mariadb_backupmate_test.sock',
            '--pid-file=/tmp/mariadb_backupmate_test.pid',
            '--log-error=/tmp/mariadb_backupmate_test.log',
            '--skip-networking=0',
            '--user=mysql',
            '--basedir=/usr',
            '--tmpdir=/tmp/mariadb_backupmate_test_tmp',
            '--innodb_data_home_dir=/tmp/mariadb_backupmate_test_innodb',
            '--innodb_log_group_home_dir=/tmp/mariadb_backupmate_test_innodb',
            '--log-bin=/tmp/mariadb_backupmate_test_tmp/mysql-bin',
            '--binlog-format=ROW',
            '--expire-logs-days=10',
            '--log-bin-index=/tmp/mariadb_backupmate_test_tmp/mysql-bin.index',
            '--open-files-limit=65535',  # Increase file limit
            '--innodb_data_file_path=ibdata1:10M:autoextend',  # Explicit InnoDB file path
            '--innodb_file_per_table=1',  # Create separate files for each table
            '--lower_case_table_names=0',  # Case sensitive
            '--log-warnings=2'  # Increase warning verbosity
        ]

        cls.mariadb_process = subprocess.Popen(server_cmd)
        
        # Wait for server to start
        max_retries = 30
        retry_interval = 1
        server_started = False
        
        for _ in range(max_retries):
            try:
                subprocess.run(
                    [
                        'mariadb',
                        '--socket=/tmp/mariadb_backupmate_test.sock',
                        '--user=root',
                        '-e', 'SELECT 1'
                    ],
                    check=True,
                    capture_output=True
                )
                server_started = True
                break
            except subprocess.CalledProcessError:
                time.sleep(retry_interval)
                
        if not server_started:
            if os.path.exists('/tmp/mariadb_backupmate_test.log'):
                with open('/tmp/mariadb_backupmate_test.log', 'r') as f:
                    log_content = f.read()
                logging.error(f"MariaDB Error Log:\n{log_content}")
            raise Exception("Failed to start MariaDB server")

        # --- Add these lines: set an empty root password & flush privileges ---
        set_password_cmd = [
            'mariadb',
            '--socket=/tmp/mariadb_backupmate_test.sock',
            '--user=root',
            '-e', '''
            ALTER USER 'root'@'localhost' IDENTIFIED BY '';
            GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;
            FLUSH PRIVILEGES;
            '''
        ]
        subprocess.run(set_password_cmd, check=True)
        cls.debug_environment()

    @classmethod
    def stop_test_mariadb_instance(cls):
        """Stop the test MariaDB instance."""
        if hasattr(cls, 'mariadb_process'):
            cls.mariadb_process.terminate()
            cls.mariadb_process.wait()

    def create_test_data(self):
        """Create test database and sample data."""
        # Combine all SQL commands into a single execution
        sql_script = """
            CREATE DATABASE IF NOT EXISTS test_db;
            USE test_db;
            CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name VARCHAR(50));
            INSERT INTO users VALUES (1, "John"), (2, "Jane"), (3, "Bob");
            CREATE TABLE IF NOT EXISTS orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2));
            INSERT INTO orders VALUES (1, 1, 100.50), (2, 2, 200.75), (3, 1, 50.25);
        """
        
        cmd = [
            'mariadb',
            '--socket=/tmp/mariadb_backupmate_test.sock',
            '--user=root'
        ]
        
        # Execute all commands in a single transaction
        logging.info("Executing SQL script...")
        try:
            result = subprocess.run(
                cmd + ['-e', sql_script],
                capture_output=True,
                text=True,
                check=True
            )
            logging.info("SQL script executed successfully")
        except subprocess.CalledProcessError as e:
            logging.error("Failed to execute SQL script")
            logging.error(f"Command output: {e.stdout}")
            logging.error(f"Command error: {e.stderr}")
            if os.path.exists('/tmp/mariadb_backupmate_test.log'):
                with open('/tmp/mariadb_backupmate_test.log', 'r') as f:
                    log_content = f.read()
                logging.error(f"MariaDB Error Log:\n{log_content}")
            raise


    def cleanup_test_data(self):
        """Clean up test database."""
        cmd = [
            'mariadb',
            '--socket=/tmp/mariadb_backupmate_test.sock',
            '--user=root',
            '-e', 'DROP DATABASE IF EXISTS test_db;'
        ]
        subprocess.run(cmd, check=True)

    def verify_database_state(self):
        """Verify database is accessible and running."""
        try:
            cmd = [
                'mariadb',
                '--socket=/tmp/mariadb_backupmate_test.sock',
                '--user=root',
                '-e', 'SELECT 1;'
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            logging.error(f"Failed to verify database state: {e}")
            return False

    def test_backup_restore_cycle(self):
        """Test complete backup and restore cycle through CLI interface."""
        # Verify initial test data
        initial_data = self.get_test_data()
        self.assertTrue(initial_data, "Initial test data not found")
        
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
        
        # Drop test database to simulate data loss
        self.cleanup_test_data()
        
        # Step 3: Restore the backup
        sys.argv = ['backupmate', 'restore', latest_backup, '--copy-back']
        result = cli.main()
        self.assertEqual(result, 0, "Restore command failed")
        
        # Step 4: Verify database is accessible and data is restored correctly
        self.assertTrue(
            self.verify_database_state(),
            "Database is not accessible after restore"
        )
        
        restored_data = self.get_test_data()
        self.assertEqual(
            initial_data, 
            restored_data, 
            "Restored data does not match initial data"
        )

    def get_test_data(self):
        """Get test data from database for verification."""
        try:
            cmd = [
                'mariadb',
                '--socket=/tmp/mariadb_backupmate_test.sock', 
                '--user=root',
                'test_db',
                '-e', 'SELECT * FROM users ORDER BY id; SELECT * FROM orders ORDER BY id;'
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError:
            return None

    @classmethod
    def debug_environment(cls):
        """
        Perform automated checks on the environment to help debug 'No such file or directory' issues.
        Logs SELinux status (if any), directory permissions, and tries a test write/read.
        """
        logging.info("=== DEBUG ENVIRONMENT START ===")
        
        # 1. Check SELinux or AppArmor status
        #    (If you're on a system without SELinux, this might just fail silently)
        try:
            result = subprocess.run(['getenforce'], capture_output=True, text=True, check=True)
            logging.info(f"SELinux status: {result.stdout.strip()}")
        except FileNotFoundError:
            logging.info("SELinux 'getenforce' not found. Probably not running SELinux.")
        except subprocess.CalledProcessError as e:
            logging.info(f"SELinux check failed: {e}")
        
        # 2. Print directory ownership/permissions for TEST_DATADIR
        logging.info("Checking permissions and ownership for TEST_DATADIR:")
        try:
            ls_cmd = ['ls', '-lhd', cls.TEST_DATADIR]
            result = subprocess.run(ls_cmd, capture_output=True, text=True, check=True)
            logging.info(f"{cls.TEST_DATADIR} -> {result.stdout.strip()}")
        except Exception as e:
            logging.warning(f"Failed to run '{' '.join(ls_cmd)}': {e}")
        
        # 3. Attempt to create and remove a test file in the data directory
        test_file_path = os.path.join(cls.TEST_DATADIR, "test_write_access.tmp")
        logging.info(f"Attempting to create and remove a test file at: {test_file_path}")
        try:
            with open(test_file_path, 'w') as f:
                f.write("test")
            if os.path.exists(test_file_path):
                logging.info("Successfully created test file in data directory.")
            else:
                logging.warning("Failed to create test file, even though no exception was raised.")
            
            # Now remove it
            os.remove(test_file_path)
            if not os.path.exists(test_file_path):
                logging.info("Successfully removed test file from data directory.")
            else:
                logging.warning("Test file still exists after attempt to delete.")
        except Exception as e:
            logging.error(f"Test file write/delete failed: {e}")
        
        # 4. Check any running mariadbd processes
        logging.info("Checking for running mariadbd processes:")
        try:
            ps_cmd = ['ps', 'aux']
            result = subprocess.run(ps_cmd, capture_output=True, text=True, check=True)
            procs = [line for line in result.stdout.splitlines() if 'mariadbd' in line]
            if procs:
                for proc in procs:
                    logging.info(f"Mariadbd process found: {proc}")
            else:
                logging.warning("No 'mariadbd' process lines found in ps output.")
        except Exception as e:
            logging.error(f"Failed to retrieve process list: {e}")
        
        logging.info("=== DEBUG ENVIRONMENT END ===")


class BackupCredentialsTest(unittest.TestCase):
    """Test credentials and permissions needed for backup operations."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        
        # Load config from .backupmate.env (production config)
        cls.config = config.load_config()
        config.validate_config(cls.config)

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
                'mariadb',  # Use MariaDB client
                f'-h{self.config["DB_HOST"]}',
                f'-P{self.config["DB_PORT"]}',
                f'-u{self.config["DB_USER"]}',
                f'-p{self.config["DB_PASSWORD"]}',
                '--protocol=TCP',  # Force TCP connection
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
