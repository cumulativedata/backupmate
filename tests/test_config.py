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
        }
        self.assertTrue(config.validate_config(valid_config))

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
