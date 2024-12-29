import unittest
import os
import sys
import subprocess
import time
import shutil
import pwd
import grp
from datetime import datetime
from backupmate import cli, config, s3, logger
import backupmate.config

# unittest command
# psftp -load "acer local" -b .\upload_to_remote.psftp_commands~ ; ssh -i C:\Users\gauth\Documents\ssh\acer_nohash ganil@192.168.1.242  "cd GIT/backupmate && sudo ~/venv_backupmate/bin/python -m unittest tests.integration_backup_restore.BackupRestoreIntegrationTest"

class BackupRestoreIntegrationTest(unittest.TestCase):
    """Integration test for complete backup and restore cycle using CLI interface."""
    
    TEST_PORT = 3307  # Different port for test instance
    TEST_DATADIR = '/tmp/backupmate_test_datadir'
    TEST_EXTRACTDIR = '/tmp/backupmate_test_extractdir'
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Configure logging using our custom logger
        cls.logger = logger.setup_logger("integration_test")
        
        config.integration_overrides = {
            'DB_PORT': str(cls.TEST_PORT),  # Convert port to string
            'MARIADB_SOCKET': f'{cls.TEST_DATADIR}.sock',  # Custom socket file
            'MARIADB_DATADIR': cls.TEST_DATADIR,  # Custom data directory
            'INNODB_DATA_HOME_DIR': cls.TEST_DATADIR+'_innodb',  # InnoDB data directory
            'INNODB_LOG_GROUP_HOME_DIR': cls.TEST_DATADIR+'_innodb',  # InnoDB log directory
            'DB_USER': 'root',
            'DB_PASSWORD': '',
            'LOCAL_TEMP_DIR': '/tmp/backupmate_test_backup',  # Separate backup directory
            'IS_INTEGRATION_TEST': True,  # Flag to enable test instance verification
            'MYSQL_START_COMMAND': f'mariadbd --datadir={cls.TEST_DATADIR} --port={cls.TEST_PORT} --socket={cls.TEST_DATADIR}.sock --pid-file={cls.TEST_DATADIR}.pid --log-error={cls.TEST_DATADIR}.log --skip-networking=0 --user=mysql --basedir=/usr --tmpdir={cls.TEST_DATADIR}_tmp --innodb_data_home_dir={cls.TEST_DATADIR}_innodb --innodb_log_group_home_dir={cls.TEST_DATADIR}_innodb --binlog-format=ROW --expire-logs-days=10 --open-files-limit=65535 --innodb_data_file_path=ibdata1:10M:autoextend --innodb_file_per_table=1 --lower_case_table_names=0 --log-warnings=2 &',
            'MYSQL_STOP_COMMAND': f'kill -9 $(cat {cls.TEST_DATADIR}.pid)'
        }
        # Load config from .backupmate.env and modify for test instance
        cls.config = config.load_config()
        

        # Update config for test instance
        config.validate_config(cls.config)
        
        # Store original sys.argv
        cls.original_argv = sys.argv
        
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        #cls.stop_test_mariadb_instance()
        #cls.cleanup_test_files() # Keeping this off for testing
        pass

    def setUp(self):
        """Set up test case."""
        pass
        
    def tearDown(self):
        """Clean up after test."""
        # Restore original sys.argv
        sys.argv = self.original_argv
        # Clean up test data
        # self.cleanup_test_data()

    @classmethod
    def set_directory_permissions(cls, directories, user='mysql', group='mysql', mode=0o750):
        """Set ownership and permissions for given directories recursively."""
        try:
            uid = pwd.getpwnam(user).pw_uid
            gid = grp.getgrnam(group).gr_gid
        except KeyError as e:
            cls.logger.error(f"User or group does not exist: {e}")
            raise

        for dir_path in directories:
            cls.logger.info(f"Setting ownership and permissions for: {dir_path}")
            try:
                # Recursively set ownership
                subprocess.run(['chown', '-R', f'{user}:{group}', dir_path], check=True)
                # Recursively set permissions
                subprocess.run(['chmod', '-R', oct(mode)[2:], dir_path], check=True)
                cls.logger.info(f"Ownership set to {user}:{group} and permissions set to {oct(mode)} for: {dir_path}")
            except subprocess.CalledProcessError as e:
                cls.logger.error(f"Failed to set permissions for {dir_path}: {e}")
                raise

    @classmethod
    def set_file_permissions(cls, file_paths, mode=0o660):
        """Set permissions for specific files."""
        for file_path in file_paths:
            cls.logger.info(f"Setting permissions for file: {file_path}")
            try:
                subprocess.run(['chmod', oct(mode)[2:], file_path], check=True)
                cls.logger.info(f"Permissions set to {oct(mode)} for: {file_path}")
            except subprocess.CalledProcessError as e:
                cls.logger.error(f"Failed to set permissions for {file_path}: {e}")
                raise

    @classmethod
    def verify_permissions(cls, paths, expected_user='mysql', expected_group='mysql', expected_mode=None):
        """Verify ownership and permissions of given paths."""
        for path in paths:
            cls.logger.info(f"Verifying permissions for: {path}")
            try:
                stat_info = os.stat(path)
                actual_user = pwd.getpwuid(stat_info.st_uid).pw_name
                actual_group = grp.getgrgid(stat_info.st_gid).gr_name
                actual_mode = stat_info.st_mode & 0o777

                if actual_user != expected_user or actual_group != expected_group:
                    cls.logger.error(f"Ownership mismatch for {path}: expected {expected_user}:{expected_group}, got {actual_user}:{actual_group}")
                    raise AssertionError(f"Ownership mismatch for {path}")

                if expected_mode and actual_mode != expected_mode:
                    cls.logger.error(f"Permission mismatch for {path}: expected {oct(expected_mode)}, got {oct(actual_mode)}")
                    raise AssertionError(f"Permission mismatch for {path}")

                cls.logger.info(f"Ownership and permissions correct for: {path}")
            except Exception as e:
                cls.logger.error(f"Failed to verify permissions for {path}: {e}")
                raise

    @classmethod
    def cleanup_test_files(cls):
        """Clean up any existing test files."""
        test_files = [
            cls.TEST_DATADIR,
            cls.TEST_EXTRACTDIR,
            f'{cls.TEST_DATADIR}.sock',
            f'{cls.TEST_DATADIR}.pid',
            f'{cls.TEST_DATADIR}.log',
            f'{cls.TEST_DATADIR}_tmp',
            f'{cls.TEST_DATADIR}_innodb'
        ]
        for file in test_files:
            try:
                if os.path.isdir(file):
                    shutil.rmtree(file)
                elif os.path.exists(file):
                    os.remove(file)
            except Exception as e:
                cls.logger.warning(f"Failed to remove {file}: {e}")
        os.system('ls -la /tmp/')
    
    @classmethod
    def verify_data_directory(cls):
        essential_files = [
            'mysql',
            'ibdata1',
            'ib_logfile0',
            'ib_logfile1',
            'aria_log.00000001',
            'aria_log_control',
            'aria_page.00000001',
            'aria_page_control'
        ]
        for file in essential_files:
            file_path = os.path.join(cls.TEST_DATADIR, file)
            cls.assertTrue(
                os.path.exists(file_path),
                f"Essential file or directory missing: {file_path}"
            )
            cls.logger.info(f"Verified existence of: {file_path}")
    
    @classmethod
    def list_data_directory_contents(cls):
        cls.logger.info(f"Listing contents of {cls.TEST_DATADIR}:")
        for root, dirs, files in os.walk(cls.TEST_DATADIR):
            for name in dirs:
                cls.logger.info(f"DIR: {os.path.join(root, name)}")
            for name in files:
                cls.logger.info(f"FILE: {os.path.join(root, name)}")

    @classmethod
    def setup_test_mariadb_instance(cls):
        """Start a new MariaDB instance for testing."""
        # Check for and kill any existing mysql process on our test port
        try:
            result = subprocess.run(['lsof', '-i', f':{cls.TEST_PORT}'], capture_output=True, text=True)
            if result.stdout:
                # Extract PID and kill the process
                for line in result.stdout.splitlines()[1:]:  # Skip header line
                    pid = line.split()[1]
                    cls.logger.info(f"Killing existing mysql process on port {cls.TEST_PORT} (PID: {pid})")
                    subprocess.run(['kill', '-9', pid], check=True)
                    time.sleep(1)  # Give process time to die
        except Exception as e:
            cls.logger.warning(f"Failed to check/kill existing mysql process: {e}")

        # Clean up any existing files first
        cls.cleanup_test_files()
        
        # Ensure mysql user exists
        try:
            subprocess.run(['id', 'mysql'], check=True)
        except subprocess.CalledProcessError:
            cls.logger.error("MySQL user does not exist. Please create it with: useradd mysql")
            raise
            
        # Create required directories
        dirs = [
            cls.TEST_DATADIR,
            f'{cls.TEST_DATADIR}_tmp',
            f'{cls.TEST_DATADIR}_innodb'
        ]
        for dir_path in dirs:
            cls.logger.info(f"Creating directory: {dir_path}")
            try:
                os.makedirs(dir_path, exist_ok=True)
                cls.logger.info(f"Directory created: {dir_path}")
            except Exception as e:
                cls.logger.error(f"Failed to setup directory {dir_path}: {str(e)}")
                raise

        # Set ownership and permissions using helper method
        cls.set_directory_permissions(dirs, user='mysql', group='mysql', mode=0o750)
        # Verify permissions were set correctly
        cls.verify_permissions(dirs, expected_user='mysql', expected_group='mysql', expected_mode=0o750)
        
        # Initialize MariaDB data directory with explicit permissions
        init_cmd = [
            'mariadb-install-db',
            f'--datadir={cls.TEST_DATADIR}',
            f'--innodb-data-home-dir={cls.TEST_DATADIR}_innodb',
            f'--innodb-log-group-home-dir={cls.TEST_DATADIR}_innodb',
            '--user=mysql',
            '--skip-test-db',
            '--auth-root-authentication-method=normal'
        ]
        try:
            result = subprocess.run(init_cmd, capture_output=True, text=True, check=True)
            cls.logger.info("MariaDB data directory initialized successfully")
            cls.logger.info(f"Initialization output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            cls.logger.error(f"Failed to initialize MariaDB data directory: {e}")
            cls.logger.error(f"Command output: {e.stdout}")
            cls.logger.error(f"Command error: {e.stderr}")
            raise

        # Verify essential files
        cls.verify_data_directory()
        cls.list_data_directory_contents()
            
        # Ensure mysql user has full permissions on data directory using helper method
        cls.set_directory_permissions([cls.TEST_DATADIR,f'{cls.TEST_DATADIR}_innodb'], user='mysql', group='mysql', mode=0o750)
        cls.verify_permissions([cls.TEST_DATADIR,f'{cls.TEST_DATADIR}_innodb'], expected_user='mysql', expected_group='mysql', expected_mode=0o750)
        
        # Create and set permissions for log file
        log_file = f'{cls.TEST_DATADIR}.log'
        try:
            # Create empty log file if it doesn't exist
            open(log_file, 'a').close()
            # Set ownership and permissions
            subprocess.run(['chown', 'mysql:mysql', log_file], check=True)
            subprocess.run(['chmod', '640', log_file], check=True)
            cls.logger.info(f"Created and set permissions for log file: {log_file}")
        except Exception as e:
            cls.logger.error(f"Failed to setup log file: {e}")
            raise

        # Start MariaDB server in background
        try:
            subprocess.run(config.integration_overrides['MYSQL_START_COMMAND'], shell=True, check=True)
            cls.logger.info("Started MariaDB server process in background")
        except subprocess.CalledProcessError as e:
            cls.logger.error(f"Failed to start MariaDB server: {e}")
            raise
        
        # Wait for server to start
        max_retries = 30
        retry_interval = 1
        server_started = False
        
        for _ in range(max_retries):
            try:
                subprocess.run(
                    [
                        'mariadb',
                        f'--socket={cls.TEST_DATADIR}.sock',
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
            if os.path.exists(f'{cls.TEST_DATADIR}.log'):
                with open(f'{cls.TEST_DATADIR}.log', 'r') as f:
                    log_content = f.read()
                cls.logger.error(f"MariaDB Error Log:\n{log_content}")
            raise Exception("Failed to start MariaDB server")

        # Set up root and backup users with proper permissions
        setup_users_cmd = [
            'mariadb',
            f'--socket={cls.TEST_DATADIR}.sock',
            '--user=root',
            '-e', '''
            -- Set empty root password and grant privileges
            ALTER USER 'root'@'localhost' IDENTIFIED BY '';
            GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;
            
            -- Create and configure backup user
            CREATE USER IF NOT EXISTS 'backupmate'@'localhost' IDENTIFIED BY 'w459vjwergpcstmjhbsp054';
            GRANT RELOAD, LOCK TABLES, PROCESS, REPLICATION CLIENT ON *.* TO 'backupmate'@'localhost';
            FLUSH PRIVILEGES;
            '''
        ]
        subprocess.run(setup_users_cmd, check=True)
        cls.debug_environment()

    @classmethod
    def stop_test_mariadb_instance(cls):
        """Stop the test MariaDB instance."""
        try:
            subprocess.run(config.integration_overrides['MYSQL_STOP_COMMAND'], shell=True, check=True)
        except Exception as e:
            cls.logger.warning(f"Failed to stop MariaDB instance: {e}")

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
            f'--socket={self.TEST_DATADIR}.sock',
            '--user=root'
        ]
        
        # Execute all commands in a single transaction
        self.__class__.logger.info("Executing SQL script...")
        print(f"Executing command: {' '.join(cmd + ['-e', sql_script])}")
        try:
            result = subprocess.run(
                cmd + ['-e', sql_script],
                capture_output=True,
                text=True,
                check=True
            )
            print(f"SQL stdout: {result.stdout}")
            print(f"SQL stderr: {result.stderr}")
            self.__class__.logger.info("SQL script executed successfully")
        except subprocess.CalledProcessError as e:
            self.__class__.logger.error("Failed to execute SQL script")
            self.__class__.logger.error(f"Command output: {e.stdout}")
            self.__class__.logger.error(f"Command error: {e.stderr}")
            if os.path.exists(f'{cls.TEST_DATADIR}.log'):
                with open(f'{cls.TEST_DATADIR}.log', 'r') as f:
                    log_content = f.read()
                self.__class__.logger.error(f"MariaDB Error Log:\n{log_content}")
            raise


    def cleanup_test_data(self):
        """Clean up test database."""
        cmd = [
            'mariadb',
            f'--socket={self.TEST_DATADIR}.sock',
            '--user=root',
            '-e', 'DROP DATABASE IF EXISTS test_db;'
        ]
        subprocess.run(cmd, check=True)

    def verify_database_state(self):
        """Verify database is accessible and running."""
        try:
            cmd = [
                'mariadb',
                f'--socket={self.TEST_DATADIR}.sock',
                '--user=root',
                '-e', 'SELECT 1;'
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            self.__class__.logger.error(f"Failed to verify database state: {e}")
            return False

    def test_incremental_backup_preparation(self):
        """Test that incremental backups are prepared correctly according to MariaDB documentation."""
        # Start test MariaDB instance
        self.setup_test_mariadb_instance()
        self.logger.info("Test MariaDB instance started")

        # Create initial test data
        self.create_test_data()
        initial_data = self.get_test_data()
        self.assertTrue(initial_data, "Initial test data not found")
        
        # Take full backup
        os.environ['IS_INTEGRATION_TEST'] = 'true'
        sys.argv = ['backupmate', 'backup', '--full']
        result = cli.main()
        os.environ.pop('IS_INTEGRATION_TEST', None)
        self.assertEqual(result, 0, "Full backup command failed")
        
        # Sleep for 1 second
        time.sleep(1)
        
        # Make some changes to the data
        sql_script1 = """
            USE test_db;
            INSERT INTO users VALUES (4, "Alice"), (5, "Charlie");
            INSERT INTO orders VALUES (4, 4, 150.75), (5, 5, 300.25);
        """
        subprocess.run(
            ['mariadb', f'--socket={self.TEST_DATADIR}.sock', '--user=root', '-e', sql_script1],
            check=True
        )
        
        # Take first incremental backup
        os.environ['IS_INTEGRATION_TEST'] = 'true'
        sys.argv = ['backupmate', 'backup']  # Incremental is default when --full is not specified
        result = cli.main()
        os.environ.pop('IS_INTEGRATION_TEST', None)
        self.assertEqual(result, 0, "First incremental backup command failed")
        
        # Sleep for 1 second
        time.sleep(1)
        
        # Make more changes to the data
        sql_script2 = """
            USE test_db;
            INSERT INTO users VALUES (6, "Dave"), (7, "Eve");
            INSERT INTO orders VALUES (6, 6, 200.00), (7, 7, 250.50);
        """
        subprocess.run(
            ['mariadb', f'--socket={self.TEST_DATADIR}.sock', '--user=root', '-e', sql_script2],
            check=True
        )
        
        # Take second incremental backup
        os.environ['IS_INTEGRATION_TEST'] = 'true'
        sys.argv = ['backupmate', 'backup']  # Incremental is default when --full is not specified
        result = cli.main()
        os.environ.pop('IS_INTEGRATION_TEST', None)
        self.assertEqual(result, 0, "Second incremental backup command failed")
        
        # Sleep for 1 second
        time.sleep(1)
        
        # Get the latest backup ID from the list command output
        import json
        import io
        from contextlib import redirect_stdout

        # Capture the output of list command
        output = io.StringIO()
        with redirect_stdout(output):
            sys.argv = ['backupmate', 'list', '--json']
            result = cli.main()
        self.assertEqual(result, 0, "List command failed")
        
        # Parse the JSON output
        try:
            backups = json.loads(output.getvalue())
            # Get the latest incremental backup
            incremental_backups = [b for b in backups if b['type'] == 'incremental']
            self.assertTrue(len(incremental_backups) > 0, "No incremental backups found")
            latest_backup = incremental_backups[-1]['id']
            self.logger.info(f"Using latest incremental backup: {latest_backup}")
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse backup list output: {e}\nOutput was: {output.getvalue()}")
        except (KeyError, IndexError) as e:
            raise Exception(f"Unexpected backup list format: {e}\nOutput was: {output.getvalue()}")
        
        # Clean up test data to simulate data loss
        self.cleanup_test_data()
        
        # Restore using the latest incremental backup
        # This will automatically prepare the full backup and apply incrementals
        os.environ['IS_INTEGRATION_TEST'] = 'true'
        sys.argv = ['backupmate', 'restore', latest_backup, '--copy-back']
        result = cli.main()
        os.environ.pop('IS_INTEGRATION_TEST', None)
        self.assertEqual(result, 0, "Restore command failed")
        
        # Verify all data is present after restore
        restored_data = self.get_test_data()
        self.assertTrue('Dave' in restored_data and 'Eve' in restored_data,
                       "Latest data not found in restored database")

    def test_backup_restore_cycle_with_backup_id(self):
        """Test backup and restore cycle using specific backup ID."""
                # Start test MariaDB instance
        self.setup_test_mariadb_instance()
        self.logger.info("Done with setup_test-mariadb_isntances")

        # Create unique timestamp for this test run
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # Create test data
        self.create_test_data()

        # Verify initial test data
        initial_data = self.get_test_data()
        self.assertTrue(initial_data, "Initial test data not found")
        
        # Step 1: Perform full backup
        os.environ['IS_INTEGRATION_TEST'] = 'true'
        sys.argv = ['backupmate', 'backup', '--full']
        result = cli.main()
        os.environ.pop('IS_INTEGRATION_TEST', None)
        self.assertEqual(result, 0, "Full backup command failed")
        
        # Get the latest backup ID from the list command output
        import json
        import io
        from contextlib import redirect_stdout

        # Capture the output of list command
        output = io.StringIO()
        with redirect_stdout(output):
            sys.argv = ['backupmate', 'list', '--json']
            result = cli.main()
        self.assertEqual(result, 0, "List command failed")
        
        # Parse the JSON output
        try:
            backups = json.loads(output.getvalue())
            # Get the latest full backup
            full_backups = [b for b in backups if b['type'] == 'full']
            self.assertTrue(len(full_backups) > 0, "No full backups found")
            latest_backup = full_backups[-1]['id']
            self.logger.info(f"Using latest full backup: {latest_backup}")
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse backup list output: {e}\nOutput was: {output.getvalue()}")
        except (KeyError, IndexError) as e:
            raise Exception(f"Unexpected backup list format: {e}\nOutput was: {output.getvalue()}")
        
        # Clean up backup folder after upload
        backup_folder = config.integration_overrides['LOCAL_TEMP_DIR']
        if os.path.exists(backup_folder):
            shutil.rmtree(backup_folder)
            self.logger.info(f"Cleaned up backup folder: {backup_folder}")
        
        # We don't Stop MariaDB server before restore because restore.py should do that

        # Drop test database to simulate data loss
        self.cleanup_test_data()
        
        # Step 3: Prepare directories with fully permissive permissions for restore
        try:
            # Create directories if they don't exist
            dirs = [
                self.TEST_EXTRACTDIR,
                # f'{self.TEST_DATADIR}_tmp'
            ]
            for dir_path in dirs:
                os.makedirs(dir_path, exist_ok=True)

            # Set ownership and permissions using helper method
            cls = self.__class__
            cls.set_directory_permissions(dirs, user='mysql', group='mysql', mode=0o750)
            # Verify permissions were set correctly
            cls.verify_permissions(dirs, expected_user='mysql', expected_group='mysql', expected_mode=0o750)
            
            self.logger.info("Prepared directories for restore")
        except Exception as e:
            self.logger.error(f"Failed to prepare directories for restore: {e}")
            raise

        # Update integration_overrides to use TEST_EXTRACTDIR for restore
        config.integration_overrides['LOCAL_TEMP_DIR'] = self.TEST_EXTRACTDIR
        
        # Now perform the restore
        os.environ['IS_INTEGRATION_TEST'] = 'true'
        sys.argv = ['backupmate', 'restore', latest_backup, '--copy-back']
        result = cli.main()
        os.environ.pop('IS_INTEGRATION_TEST', None)
        self.assertEqual(result, 0, "Restore command failed")

        # Verify server is running after restore
        self.logger.info("Verifying server is running after restore...")
        
        max_retries = 5
        retry_interval = 1
        server_started = False
        
        for _ in range(max_retries):
            try:
                subprocess.run(
                    [
                        'mariadb',
                        f'--socket={self.TEST_DATADIR}.sock',
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
            if os.path.exists(f'{self.TEST_DATADIR}.log'):
                with open(f'{self.TEST_DATADIR}.log', 'r') as f:
                    log_content = f.read()
                self.logger.error(f"MariaDB Error Log:\n{log_content}")
            raise Exception("Failed to start MariaDB server after restore")
        
        self.logger.info("MariaDB server started successfully after restore")
        
        
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

    # def test_backup_restore_cycle_with_latest_full(self): # TODO Later
    #     """Test backup and restore cycle using --latest-full flag."""
    #     pass 

    # def test_backup_restore_cycle_with_latest_incremental(self): # TODO Later
    #     """Test backup and restore cycle using --latest-incremental flag."""
    #     pass

    def get_test_data(self):
        """Get test data from database for verification."""
        try:
            cmd = [
                'mariadb',
                f'--socket={self.TEST_DATADIR}.sock', 
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
        cls.logger.info("=== DEBUG ENVIRONMENT START ===")
        
        # 1. Check SELinux or AppArmor status
        #    (If you're on a system without SELinux, this might just fail silently)
        try:
            result = subprocess.run(['getenforce'], capture_output=True, text=True, check=True)
            cls.logger.info(f"SELinux status: {result.stdout.strip()}")
        except FileNotFoundError:
            cls.logger.info("SELinux 'getenforce' not found. Probably not running SELinux.")
        except subprocess.CalledProcessError as e:
            cls.logger.info(f"SELinux check failed: {e}")
        
        # 2. Print directory ownership/permissions for TEST_DATADIR
        cls.logger.info("Checking permissions and ownership for TEST_DATADIR:")
        try:
            ls_cmd = ['ls', '-lhd', cls.TEST_DATADIR]
            result = subprocess.run(ls_cmd, capture_output=True, text=True, check=True)
            cls.logger.info(f"{cls.TEST_DATADIR} -> {result.stdout.strip()}")
        except Exception as e:
            cls.logger.warning(f"Failed to run '{' '.join(ls_cmd)}': {e}")
        
        # 3. Attempt to create and remove a test file in the data directory
        test_file_path = os.path.join(cls.TEST_DATADIR, "test_write_access.tmp")
        cls.logger.info(f"Attempting to create and remove a test file at: {test_file_path}")
        try:
            with open(test_file_path, 'w') as f:
                f.write("test")
            if os.path.exists(test_file_path):
                cls.logger.info("Successfully created test file in data directory.")
            else:
                cls.logger.warning("Failed to create test file, even though no exception was raised.")
            
            # Now remove it
            os.remove(test_file_path)
            if not os.path.exists(test_file_path):
                cls.logger.info("Successfully removed test file from data directory.")
            else:
                cls.logger.warning("Test file still exists after attempt to delete.")
        except Exception as e:
            cls.logger.error(f"Test file write/delete failed: {e}")
        
        # 4. Check any running mariadbd processes
        cls.logger.info("Checking for running mariadbd processes:")
        try:
            ps_cmd = ['ps', 'aux']
            result = subprocess.run(ps_cmd, capture_output=True, text=True, check=True)
            procs = [line for line in result.stdout.splitlines() if 'mariadbd' in line]
            if procs:
                for proc in procs:
                    cls.logger.info(f"Mariadbd process found: {proc}")
            else:
                cls.logger.warning("No 'mariadbd' process lines found in ps output.")
        except Exception as e:
            cls.logger.error(f"Failed to retrieve process list: {e}")
        
        cls.logger.info("=== DEBUG ENVIRONMENT END ===")


class BackupCredentialsTest(unittest.TestCase):
    """Test credentials and permissions needed for backup operations."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Configure logging using our custom logger
        cls.logger = logger.setup_logger("backup_credentials_test")
        
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
            test_backup_dir = '/tmp/backupmate_test_backup'
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
