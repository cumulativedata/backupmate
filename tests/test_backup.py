import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import logging
from datetime import datetime
from backupmate import backup

# Configure logging for tests
logging.basicConfig(level=logging.DEBUG)

class TestBackup(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        # Delete test.db if it exists
        if os.path.exists('test.db'):
            try:
                os.remove('test.db')
            except PermissionError:
                # If file is locked, wait a bit and try again
                import time
                time.sleep(0.1)
                os.remove('test.db')
        
        self.config = {
            'LOCAL_TEMP_DIR': 'C:\\temp\\backups',
            'CHAIN_DIR': 'C:\\temp\\backups\\chain',
            'FULL_BACKUP_PREFIX': 'backups/full',
            'INCREMENTAL_BACKUP_PREFIX': 'backups/incremental',
            'S3_BUCKET_NAME': 'test-bucket',
            'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup',
            'DB_HOST': 'localhost',
            'DB_PORT': '3306',
            'DB_USER': 'root',
            'DB_PASSWORD': 'password',
            'IS_TEST': True
        }
        
        # Initialize test database
        self.conn = backup._init_db(self.config)
        
    def tearDown(self):
        """Clean up test fixtures."""
        if hasattr(self, 'conn'):
            self.conn.close()
        if os.path.exists('test.db'):
            try:
                os.remove('test.db')
            except PermissionError:
                pass
        
    @patch('backupmate.backup.shutil.rmtree')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.os.makedirs')
    def test_clean_backup_chain(self, mock_makedirs, mock_exists, mock_rmtree):
        """Tests backup chain directory cleaning."""
        # Test when chain directory exists
        mock_exists.return_value = True
        backup._clean_backup_chain(self.config)
        
        mock_exists.assert_called_once_with(os.path.join(self.config['LOCAL_TEMP_DIR'], 'chain'))
        mock_rmtree.assert_called_once_with(os.path.join(self.config['LOCAL_TEMP_DIR'], 'chain'))
        mock_makedirs.assert_called_once_with(os.path.join(self.config['LOCAL_TEMP_DIR'], 'chain'), exist_ok=True)

    def test_get_latest_local_backup(self):
        """Tests retrieving latest local backup path."""
        # Test when no backup exists
        result = backup.get_latest_local_backup(self.config)
        self.assertIsNone(result)
        
        # Insert a test backup record
        cursor = self.conn.cursor()
        expected_path = 'C:\\temp\\backups\\chain\\full_20230101_000000'
        cursor.execute('''
            INSERT INTO backups (backup_type, backup_prefix, local_path, status)
            VALUES (?, ?, ?, ?)
        ''', ('full', 'test/prefix', expected_path, 'success'))
        self.conn.commit()
        
        # Test when backup exists
        result = backup.get_latest_local_backup(self.config)
        self.assertEqual(result, expected_path)

    @patch('backupmate.backup.os.makedirs')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.mariadb.take_full_backup')
    @patch('backupmate.backup.mariadb.prepare_backup')
    @patch('backupmate.backup.utils.compress_directory')
    @patch('backupmate.backup.s3.upload_file')
    @patch('backupmate.backup.record_backup_metadata')
    @patch('backupmate.backup.shutil.copytree')
    @patch('backupmate.backup.shutil.rmtree')
    def test_perform_full_backup_success(self, mock_rmtree, mock_copytree, mock_record, 
                                       mock_upload, mock_compress, mock_prepare, 
                                       mock_take_backup, mock_exists, mock_makedirs):
        """Tests successful full backup orchestration."""
        # Configure mocks
        def exists_side_effect(path):
            return True
        mock_exists.side_effect = exists_side_effect
        mock_take_backup.return_value = True
        mock_prepare.return_value = True
        mock_compress.return_value = True
        mock_upload.return_value = True
        mock_record.return_value = True
        
        # Execute test
        result = backup.perform_full_backup(self.config)
        
        # Verify
        self.assertTrue(result)
        mock_makedirs.assert_called()
        mock_take_backup.assert_called_once()
        mock_prepare.assert_called_once()
        mock_compress.assert_called_once()
        mock_upload.assert_called_once()
        mock_record.assert_called_once()
        
    @patch('backupmate.backup.os.makedirs')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.mariadb.take_full_backup')
    @patch('backupmate.backup.shutil.rmtree')
    @patch('backupmate.backup.os.remove')
    def test_perform_full_backup_failure(self, mock_remove, mock_rmtree, mock_take_backup, 
                                       mock_exists, mock_makedirs):
        """Tests full backup failure handling."""
        # Mock path existence checks
        def exists_side_effect(path):
            return True
        mock_exists.side_effect = exists_side_effect
        mock_take_backup.return_value = False
        
        result = backup.perform_full_backup(self.config)
        
        self.assertFalse(result)
        mock_take_backup.assert_called_once()
        # Verify cleanup was attempted for both chain and temp directories
        self.assertEqual(mock_rmtree.call_count, 2)
        mock_remove.assert_called_once()

    @patch('backupmate.backup.os.makedirs')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.mariadb.take_full_backup')
    @patch('backupmate.backup.mariadb.prepare_backup')
    @patch('backupmate.backup.shutil.rmtree')
    @patch('backupmate.backup.os.remove')
    def test_perform_full_backup_prepare_failure(self, mock_remove, mock_rmtree, 
                                               mock_prepare, mock_take_backup,
                                               mock_exists, mock_makedirs):
        """Tests backup preparation failure handling."""
        # Mock path existence checks
        def exists_side_effect(path):
            return True
        mock_exists.side_effect = exists_side_effect
        mock_take_backup.return_value = True
        mock_prepare.return_value = False
        
        result = backup.perform_full_backup(self.config)
        
        self.assertFalse(result)
        mock_take_backup.assert_called_once()
        mock_prepare.assert_called_once()
        # Verify cleanup was attempted for both chain and temp directories
        self.assertEqual(mock_rmtree.call_count, 2)
        mock_remove.assert_called_once()

    @patch('backupmate.backup.shutil.rmtree')
    @patch('backupmate.backup.mariadb.prepare_backup')
    @patch('backupmate.backup.mariadb.take_full_backup')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.os.makedirs')
    def test_cleanup_continues_on_error(self, mock_makedirs, mock_exists, mock_take_backup,
                                      mock_prepare, mock_rmtree):
        """Tests that cleanup continues even if some operations fail."""
        # Configure mocks
        mock_exists.return_value = True  # Simplify exists mock
        mock_makedirs.return_value = None  # makedirs returns None on success
        mock_take_backup.return_value = True
        mock_prepare.return_value = False  # Fail at prepare to trigger cleanup
        
        # Make rmtree fail only during cleanup, not during initial chain cleanup
        def rmtree_side_effect(path):
            if 'full_' in path:  # Only fail when cleaning up the backup dir
                raise OSError("Failed to remove directory")
        mock_rmtree.side_effect = rmtree_side_effect
        
        # Execute test
        result = backup.perform_full_backup(self.config)
        
        self.assertFalse(result)
        mock_take_backup.assert_called_once()
        # Should try to remove both chain and temp directories
        self.assertEqual(mock_rmtree.call_count, 2)
        
    @patch('backupmate.backup.shutil.rmtree')
    @patch('backupmate.backup.shutil.copytree')
    @patch('backupmate.backup.record_backup_metadata')
    @patch('backupmate.backup.get_latest_local_backup')
    @patch('backupmate.backup.s3.upload_file')
    @patch('backupmate.backup.utils.compress_directory')
    @patch('backupmate.backup.mariadb.take_incremental_backup')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.os.makedirs')
    def test_perform_incremental_backup_success(self, mock_makedirs, mock_exists, mock_take_backup,
                                              mock_compress, mock_upload, mock_get_latest,
                                              mock_record, mock_copytree, mock_rmtree):
        """Tests successful incremental backup orchestration with local chain."""
        base_prefix = 'backups/full/20230101_000000'  # kept for compatibility
        base_dir = 'C:\\temp\\backups\\chain\\full_20230101_000000'
        
        # Configure mocks
        def exists_side_effect(path):
            return True
        mock_exists.side_effect = exists_side_effect
        mock_get_latest.return_value = base_dir
        mock_take_backup.return_value = True
        mock_compress.return_value = True
        mock_upload.return_value = True
        mock_record.return_value = True
        
        # Execute test
        result = backup.perform_incremental_backup(self.config, base_prefix)
        
        # Verify
        self.assertTrue(result)
        mock_makedirs.assert_called()
        mock_get_latest.assert_called_once()
        mock_take_backup.assert_called_once()
        mock_compress.assert_called_once()
        mock_upload.assert_called_once()
        mock_record.assert_called_once()
        
    @patch('backupmate.backup.os.makedirs')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.get_latest_local_backup')
    @patch('backupmate.backup.shutil.rmtree')
    @patch('backupmate.backup.os.remove')
    def test_perform_incremental_backup_no_base(self, mock_remove, mock_rmtree, 
                                              mock_get_latest, mock_exists, mock_makedirs):
        """Tests incremental backup failure when no local base backup exists."""
        base_prefix = 'backups/full/20230101_000000'  # kept for compatibility
        def exists_side_effect(path):
            return True
        mock_exists.side_effect = exists_side_effect
        mock_get_latest.return_value = None
        
        result = backup.perform_incremental_backup(self.config, base_prefix)
        
        self.assertFalse(result)
        mock_get_latest.assert_called_once()
        # Verify cleanup was attempted for both directories
        self.assertEqual(mock_rmtree.call_count, 2)  # backup_dir and base_dir
        mock_remove.assert_called_once()

    @patch('backupmate.backup.os.remove')
    @patch('backupmate.backup.shutil.rmtree')
    @patch('backupmate.backup.mariadb.take_incremental_backup')
    @patch('backupmate.backup.get_latest_local_backup')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.os.makedirs')
    def test_perform_incremental_backup_mariadb_failure(self, mock_makedirs, mock_exists,
                                                      mock_get_latest, mock_take_backup,
                                                      mock_rmtree, mock_remove):
        """Tests incremental backup mariadb failure handling with local chain."""
        base_prefix = 'backups/full/20230101_000000'  # kept for compatibility
        base_dir = 'C:\\temp\\backups\\chain\\full_20230101_000000'
        def exists_side_effect(path):
            return True
        mock_exists.side_effect = exists_side_effect
        mock_get_latest.return_value = base_dir
        mock_take_backup.return_value = False
        
        result = backup.perform_incremental_backup(self.config, base_prefix)
        
        self.assertFalse(result)
        mock_get_latest.assert_called_once()
        mock_take_backup.assert_called_once()
        # Verify cleanup was attempted for both directories
        self.assertEqual(mock_rmtree.call_count, 2)  # backup_dir and base_dir
        mock_remove.assert_called_once()

    @patch('backupmate.backup.shutil.rmtree')
    @patch('backupmate.backup.mariadb.take_incremental_backup')
    @patch('backupmate.backup.get_latest_local_backup')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.os.makedirs')
    def test_incremental_cleanup_continues_on_error(self, mock_makedirs, mock_exists,
                                                  mock_get_latest, mock_take_backup,
                                                  mock_rmtree):
        """Tests that incremental backup cleanup continues even if some operations fail."""
        base_prefix = 'backups/full/20230101_000000'  # kept for compatibility
        base_dir = 'C:\\temp\\backups\\chain\\full_20230101_000000'
        
        def exists_side_effect(path):
            return True
        mock_exists.side_effect = exists_side_effect
        
        def rmtree_side_effect(path):
            raise OSError("Failed to remove directory")
        mock_rmtree.side_effect = rmtree_side_effect
        
        mock_get_latest.return_value = base_dir
        mock_take_backup.return_value = True  # Let it proceed to cleanup
        
        result = backup.perform_incremental_backup(self.config, base_prefix)
        
        self.assertFalse(result)
        mock_get_latest.assert_called_once()
        # Verify both directories were created
        self.assertEqual(mock_makedirs.call_count, 2)
        # Should try to remove both directories even if first one fails
        self.assertEqual(mock_rmtree.call_count, 2)
        
    @patch('backupmate.backup.s3.get_latest_backup_prefix')
    def test_get_latest_full_backup_prefix_success(self, mock_get_latest):
        """Tests successful retrieval of latest full backup prefix."""
        expected_prefix = 'backups/full/20230101_000000'
        mock_get_latest.return_value = expected_prefix
        
        result = backup.get_latest_full_backup_prefix(
            self.config['S3_BUCKET_NAME'],
            self.config['FULL_BACKUP_PREFIX'],
            self.config
        )
        
        self.assertEqual(result, expected_prefix)
        mock_get_latest.assert_called_once()
        
    @patch('backupmate.backup.s3.get_latest_backup_prefix')
    def test_get_latest_full_backup_prefix_failure(self, mock_get_latest):
        """Tests failure handling in latest full backup prefix retrieval."""
        mock_get_latest.side_effect = Exception("S3 error")
        
        result = backup.get_latest_full_backup_prefix(
            self.config['S3_BUCKET_NAME'],
            self.config['FULL_BACKUP_PREFIX'],
            self.config
        )
        
        self.assertIsNone(result)
        mock_get_latest.assert_called_once()
        
    def test_list_backups_from_db_success(self):
        """Tests successful backup listing from database."""
        # Initially should be empty
        result = backup.list_backups_from_db(self.config)
        self.assertEqual(result, [])
        
        # Insert some test records
        cursor = self.conn.cursor()
        test_records = [
            ('full', 'backups/full/20230101', 'success'),
            ('incremental', 'backups/inc/20230102', 'success')
        ]
        for record in test_records:
            cursor.execute('''
                INSERT INTO backups (backup_type, backup_prefix, status)
                VALUES (?, ?, ?)
            ''', record)
        self.conn.commit()
        
        # Test normal list
        result = backup.list_backups_from_db(self.config)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], 'full')  # backup_type
        self.assertEqual(result[0][1], 'backups/full/20230101')  # backup_prefix
        self.assertEqual(result[0][3], 'success')  # status
        
        # Test JSON output
        result_json = backup.list_backups_from_db(self.config, output_json=True)
        self.assertIsInstance(result_json, str)
        import json
        json_data = json.loads(result_json)
        self.assertEqual(len(json_data), 2)
        
    def test_record_backup_metadata_success(self):
        """Tests successful backup metadata recording."""
        # Record a backup
        result = backup.record_backup_metadata(
            self.config,
            'full',
            'backups/full/20230101',
            'C:\\temp\\backups\\chain\\full_20230101'
        )
        self.assertTrue(result)
        
        # Verify it was recorded
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM backups')
        records = cursor.fetchall()
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record[1], 'full')  # backup_type
        self.assertEqual(record[2], 'backups/full/20230101')  # backup_prefix
        self.assertEqual(record[3], 'C:\\temp\\backups\\chain\\full_20230101')  # local_path
        self.assertEqual(record[5], 'success')  # status

if __name__ == '__main__':
    unittest.main()
