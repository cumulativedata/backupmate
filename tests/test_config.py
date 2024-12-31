import os
import unittest
from backupmate import config

class TestConfig(unittest.TestCase):
    def test_load_config(self):
        """Tests if the configuration is loaded correctly."""
        # Create a temporary .env file for testing
        with open(".backupmate.env", "w") as f:
            f.write("DB_HOST=localhost\n")
            f.write("DB_PORT=3306\n")
            f.write("DB_USER=testuser\n")
            f.write("DB_PASSWORD=testpassword\n")
            f.write("MARIADB_BACKUP_PATH=/usr/bin/mariabackup\n")
            f.write("S3_BUCKET_NAME=test-bucket\n")
            f.write("AWS_ACCESS_KEY_ID=TEST_KEY_ID\n")
            f.write("AWS_SECRET_ACCESS_KEY=TEST_SECRET_KEY\n")
            f.write("AWS_REGION=us-east-1\n")
            f.write("LOCAL_TEMP_DIR=/tmp/backupmate\n")
            f.write("FULL_BACKUP_PREFIX=backupmate/full/\n")
            f.write("INCREMENTAL_BACKUP_PREFIX=backupmate/incremental/\n")
            f.write("FULL_BACKUP_SCHEDULE=weekly\n")
            f.write("SQLITE_FILE=backupmate.db\n")

        loaded_config = config.load_config()
        self.assertEqual(loaded_config["DB_HOST"], "localhost")
        self.assertEqual(loaded_config["DB_PORT"], "3306")
        self.assertEqual(loaded_config["DB_USER"], "testuser")
        self.assertEqual(loaded_config["DB_PASSWORD"], "testpassword")
        self.assertEqual(loaded_config["MARIADB_BACKUP_PATH"], "/usr/bin/mariabackup")
        self.assertEqual(loaded_config["S3_BUCKET_NAME"], "test-bucket")
        self.assertEqual(loaded_config["AWS_ACCESS_KEY_ID"], "TEST_KEY_ID")
        self.assertEqual(loaded_config["AWS_SECRET_ACCESS_KEY"], "TEST_SECRET_KEY")
        self.assertEqual(loaded_config["AWS_REGION"], "us-east-1")
        self.assertEqual(loaded_config["LOCAL_TEMP_DIR"], "/tmp/backupmate")
        self.assertEqual(loaded_config["FULL_BACKUP_PREFIX"], "backupmate/full/")
        self.assertEqual(loaded_config["INCREMENTAL_BACKUP_PREFIX"], "backupmate/incremental/")
        self.assertEqual(loaded_config["FULL_BACKUP_SCHEDULE"], "weekly")

        # Clean up the temporary .env file
        os.remove(".backupmate.env")

    def test_validate_config_success(self):
        """Tests if valid configuration passes validation."""
        valid_config = {
            "DB_HOST": "localhost",
            "DB_PORT": "3306",
            "DB_USER": "testuser",
            "DB_PASSWORD": "testpassword",
            "MARIADB_BACKUP_PATH": "/usr/bin/mariabackup",
            "S3_BUCKET_NAME": "test-bucket",
            "AWS_ACCESS_KEY_ID": "TEST_KEY_ID",
            "AWS_SECRET_ACCESS_KEY": "TEST_SECRET_KEY",
            "AWS_REGION": "us-east-1",
            "LOCAL_TEMP_DIR": "/tmp/backupmate",
            "FULL_BACKUP_PREFIX": "backupmate/full/",
            "INCREMENTAL_BACKUP_PREFIX": "backupmate/incremental/",
            "FULL_BACKUP_SCHEDULE": "weekly",
            "SQLITE_FILE": "/var/lib/backupmate/backupmate.db",
        }
        self.assertTrue(config.validate_config(valid_config))

    def test_load_config_file_not_found(self):
        """Tests if loading from a non-existent .env file returns empty config."""
        config_result = config.load_config("nonexistent.env")
        for value in config_result.values():
            self.assertIsNone(value)

    def test_load_config_custom_path(self):
        """Tests loading config from a custom path."""
        # Create a temporary .env file in a custom location
        custom_path = "custom.env"
        with open(custom_path, "w") as f:
            f.write("DB_HOST=customhost\n")
            f.write("DB_PORT=3307\n")
            # Add other required fields with empty values
            f.write("DB_USER=\n")
            f.write("DB_PASSWORD=\n")
            f.write("MARIADB_BACKUP_PATH=\n")
            f.write("S3_BUCKET_NAME=\n")
            f.write("AWS_ACCESS_KEY_ID=\n")
            f.write("AWS_SECRET_ACCESS_KEY=\n")
            f.write("AWS_REGION=\n")
            f.write("LOCAL_TEMP_DIR=\n")
            f.write("FULL_BACKUP_PREFIX=\n")
            f.write("INCREMENTAL_BACKUP_PREFIX=\n")
            f.write("FULL_BACKUP_SCHEDULE=\n")

        loaded_config = config.load_config(custom_path)
        self.assertEqual(loaded_config["DB_HOST"], "customhost")
        self.assertEqual(loaded_config["DB_PORT"], "3307")
        # Clean up
        os.remove(custom_path)

    def test_validate_config_empty_values(self):
        """Tests validation of config with empty values."""
        empty_config = {
            "DB_HOST": "",
            "DB_PORT": "3306",
            "DB_USER": "",
            "DB_PASSWORD": "",
            "MARIADB_BACKUP_PATH": "",
            "S3_BUCKET_NAME": "",
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_REGION": "",
            "LOCAL_TEMP_DIR": "",
            "FULL_BACKUP_PREFIX": "",
            "INCREMENTAL_BACKUP_PREFIX": "",
            "FULL_BACKUP_SCHEDULE": "",
        }
        with self.assertRaises(ValueError) as context:
            config.validate_config(empty_config)
        self.assertIn("Missing required configuration parameter:", str(context.exception))

    def test_validate_config_schedule_format(self):
        """Tests validation of backup schedule format."""
        invalid_schedule_config = {
            "DB_HOST": "localhost",
            "DB_PORT": "3306",
            "DB_USER": "testuser",
            "DB_PASSWORD": "testpassword",
            "MARIADB_BACKUP_PATH": "/usr/bin/mariabackup",
            "S3_BUCKET_NAME": "test-bucket",
            "AWS_ACCESS_KEY_ID": "TEST_KEY_ID",
            "AWS_SECRET_ACCESS_KEY": "TEST_SECRET_KEY",
            "AWS_REGION": "us-east-1",
            "LOCAL_TEMP_DIR": "/tmp/backupmate",
            "FULL_BACKUP_PREFIX": "backupmate/full/",
            "INCREMENTAL_BACKUP_PREFIX": "backupmate/incremental/",
            "FULL_BACKUP_SCHEDULE": "invalid",  # Should be weekly or monthly
        }
        with self.assertRaises(ValueError) as context:
            config.validate_config(invalid_schedule_config)
        self.assertIn("FULL_BACKUP_SCHEDULE must be either 'weekly' or 'monthly'", str(context.exception))

    def test_validate_config_s3_paths(self):
        """Tests validation of S3 prefix paths."""
        invalid_prefix_config = {
            "DB_HOST": "localhost",
            "DB_PORT": "3306",
            "DB_USER": "testuser",
            "DB_PASSWORD": "testpassword",
            "MARIADB_BACKUP_PATH": "/usr/bin/mariabackup",
            "S3_BUCKET_NAME": "test-bucket",
            "AWS_ACCESS_KEY_ID": "TEST_KEY_ID",
            "AWS_SECRET_ACCESS_KEY": "TEST_SECRET_KEY",
            "AWS_REGION": "us-east-1",
            "LOCAL_TEMP_DIR": "/tmp/backupmate",
            "FULL_BACKUP_PREFIX": "backupmate/full",  # Missing trailing slash
            "INCREMENTAL_BACKUP_PREFIX": "backupmate/incremental",  # Missing trailing slash
            "FULL_BACKUP_SCHEDULE": "weekly",
        }
        with self.assertRaises(ValueError) as context:
            config.validate_config(invalid_prefix_config)
        self.assertIn("S3 prefix paths must end with '/'", str(context.exception))

    def test_sqlite_file_default(self):
        """Tests if SQLITE_FILE has correct default value."""
        # Create minimal config without SQLITE_FILE
        with open(".backupmate.env", "w") as f:
            f.write("DB_HOST=localhost\n")
            
        try:
            loaded_config = config.load_config()
            self.assertEqual(loaded_config["SQLITE_FILE"], "backupmate.db")
        finally:
            os.remove(".backupmate.env")

    def test_validate_config_local_paths(self):
        """Tests validation of local directory paths."""
        invalid_path_config = {
            "DB_HOST": "localhost",
            "DB_PORT": "3306",
            "DB_USER": "testuser",
            "DB_PASSWORD": "testpassword",
            "MARIADB_BACKUP_PATH": "mariabackup",  # Should be absolute path
            "S3_BUCKET_NAME": "test-bucket",
            "AWS_ACCESS_KEY_ID": "TEST_KEY_ID",
            "AWS_SECRET_ACCESS_KEY": "TEST_SECRET_KEY",
            "AWS_REGION": "us-east-1",
            "LOCAL_TEMP_DIR": "tmp/backupmate",  # Should be absolute path
            "FULL_BACKUP_PREFIX": "backupmate/full/",
            "INCREMENTAL_BACKUP_PREFIX": "backupmate/incremental/",
            "FULL_BACKUP_SCHEDULE": "weekly",
        }
        with self.assertRaises(ValueError) as context:
            config.validate_config(invalid_path_config)
        self.assertIn("must be an absolute path", str(context.exception))

    def test_validate_config_failure(self):
        """Tests if invalid configuration raises an exception."""
        invalid_config = {
            "DB_HOST": "localhost",
            # Missing DB_PORT
            "DB_USER": "testuser",
            "DB_PASSWORD": "testpassword",
            "MARIADB_BACKUP_PATH": "/usr/bin/mariabackup",
            "S3_BUCKET_NAME": "test-bucket",
            "AWS_ACCESS_KEY_ID": "TEST_KEY_ID",
            "AWS_SECRET_ACCESS_KEY": "TEST_SECRET_KEY",
            "AWS_REGION": "us-east-1",
            "LOCAL_TEMP_DIR": "/tmp/backupmate",
            "FULL_BACKUP_PREFIX": "backupmate/full/",
            "INCREMENTAL_BACKUP_PREFIX": "backupmate/incremental/",
            "FULL_BACKUP_SCHEDULE": "weekly",
        }
        with self.assertRaises(ValueError) as context:
            config.validate_config(invalid_config)
        self.assertIn("Missing required configuration parameter: DB_PORT", str(context.exception))

        invalid_port_config = {
            "DB_HOST": "localhost",
            "DB_PORT": "invalid",
            "DB_USER": "testuser",
            "DB_PASSWORD": "testpassword",
            "MARIADB_BACKUP_PATH": "/usr/bin/mariabackup",
            "S3_BUCKET_NAME": "test-bucket",
            "AWS_ACCESS_KEY_ID": "TEST_KEY_ID",
            "AWS_SECRET_ACCESS_KEY": "TEST_SECRET_KEY",
            "AWS_REGION": "us-east-1",
            "LOCAL_TEMP_DIR": "/tmp/backupmate",
            "FULL_BACKUP_PREFIX": "backupmate/full/",
            "INCREMENTAL_BACKUP_PREFIX": "backupmate/incremental/",
            "FULL_BACKUP_SCHEDULE": "weekly",
        }
        with self.assertRaises(ValueError) as context:
            config.validate_config(invalid_port_config)
        self.assertIn("DB_PORT must be an integer", str(context.exception))

if __name__ == '__main__':
    unittest.main()
