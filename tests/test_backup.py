import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
from datetime import datetime
from backupmate import backup

class TestBackup(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            'LOCAL_TEMP_DIR': 'C:\\temp\\backups',
            'FULL_BACKUP_PREFIX': 'backups/full',
            'INCREMENTAL_BACKUP_PREFIX': 'backups/incremental',
            'S3_BUCKET_NAME': 'test-bucket',
            'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup',
            'DB_HOST': 'localhost',
            'DB_PORT': '3306',
            'DB_USER': 'root',
            'DB_PASSWORD': 'password'
        }
        
    @patch('backupmate.backup.os.makedirs')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.mariadb.take_full_backup')
    @patch('backupmate.backup.mariadb.prepare_backup')
    @patch('backupmate.backup.utils.compress_directory')
    @patch('backupmate.backup.s3.upload_directory')
    @patch('backupmate.backup.record_backup_metadata')
    def test_perform_full_backup_success(self, mock_record, mock_upload, mock_compress, 
                                       mock_prepare, mock_take_backup, mock_exists, 
                                       mock_makedirs):
        """Tests successful full backup orchestration."""
        # Configure mocks
        mock_exists.return_value = True
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
        mock_exists.return_value = True
        mock_take_backup.return_value = False
        
        result = backup.perform_full_backup(self.config)
        
        self.assertFalse(result)
        mock_take_backup.assert_called_once()
        # Verify cleanup was attempted
        mock_rmtree.assert_called_once()
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
        mock_exists.return_value = True
        mock_take_backup.return_value = True
        mock_prepare.return_value = False
        
        result = backup.perform_full_backup(self.config)
        
        self.assertFalse(result)
        mock_take_backup.assert_called_once()
        mock_prepare.assert_called_once()
        # Verify cleanup was attempted
        mock_rmtree.assert_called_once()
        mock_remove.assert_called_once()

    @patch('backupmate.backup.os.makedirs')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.mariadb.take_full_backup')
    @patch('backupmate.backup.shutil.rmtree')
    def test_cleanup_continues_on_error(self, mock_rmtree, mock_take_backup,
                                      mock_exists, mock_makedirs):
        """Tests that cleanup continues even if some operations fail."""
        mock_exists.return_value = True
        mock_take_backup.return_value = False
        mock_rmtree.side_effect = OSError("Failed to remove directory")
        
        result = backup.perform_full_backup(self.config)
        
        self.assertFalse(result)
        mock_take_backup.assert_called_once()
        mock_rmtree.assert_called_once()
        
    @patch('backupmate.backup.os.makedirs')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.mariadb.take_incremental_backup')
    @patch('backupmate.backup.utils.compress_directory')
    @patch('backupmate.backup.s3.upload_directory')
    @patch('backupmate.backup.s3.download_directory')
    @patch('backupmate.backup.record_backup_metadata')
    def test_perform_incremental_backup_success(self, mock_record, mock_download, 
                                              mock_upload, mock_compress, mock_take_backup,
                                              mock_exists, mock_makedirs):
        """Tests successful incremental backup orchestration."""
        base_prefix = 'backups/full/20230101_000000'
        
        # Configure mocks
        mock_exists.return_value = True
        mock_download.return_value = True
        mock_take_backup.return_value = True
        mock_compress.return_value = True
        mock_upload.return_value = True
        mock_record.return_value = True
        
        # Execute test
        result = backup.perform_incremental_backup(self.config, base_prefix)
        
        # Verify
        self.assertTrue(result)
        mock_makedirs.assert_called()
        mock_download.assert_called_once()
        mock_take_backup.assert_called_once()
        mock_compress.assert_called_once()
        mock_upload.assert_called_once()
        mock_record.assert_called_once()
        
    @patch('backupmate.backup.os.makedirs')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.s3.download_directory')
    @patch('backupmate.backup.shutil.rmtree')
    @patch('backupmate.backup.os.remove')
    def test_perform_incremental_backup_failure(self, mock_remove, mock_rmtree, 
                                              mock_download, mock_exists, mock_makedirs):
        """Tests incremental backup failure handling."""
        base_prefix = 'backups/full/20230101_000000'
        mock_exists.return_value = True
        mock_download.return_value = False
        
        result = backup.perform_incremental_backup(self.config, base_prefix)
        
        self.assertFalse(result)
        mock_download.assert_called_once()
        # Verify cleanup was attempted for both directories
        self.assertEqual(mock_rmtree.call_count, 2)  # backup_dir and base_dir
        mock_remove.assert_called_once()

    @patch('backupmate.backup.os.makedirs')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.s3.download_directory')
    @patch('backupmate.backup.mariadb.take_incremental_backup')
    @patch('backupmate.backup.shutil.rmtree')
    @patch('backupmate.backup.os.remove')
    def test_perform_incremental_backup_mariadb_failure(self, mock_remove, mock_rmtree,
                                                      mock_take_backup, mock_download,
                                                      mock_exists, mock_makedirs):
        """Tests incremental backup mariadb failure handling."""
        base_prefix = 'backups/full/20230101_000000'
        mock_exists.return_value = True
        mock_download.return_value = True
        mock_take_backup.return_value = False
        
        result = backup.perform_incremental_backup(self.config, base_prefix)
        
        self.assertFalse(result)
        mock_download.assert_called_once()
        mock_take_backup.assert_called_once()
        # Verify cleanup was attempted for both directories
        self.assertEqual(mock_rmtree.call_count, 2)  # backup_dir and base_dir
        mock_remove.assert_called_once()

    @patch('backupmate.backup.os.makedirs')
    @patch('backupmate.backup.os.path.exists')
    @patch('backupmate.backup.s3.download_directory')
    @patch('backupmate.backup.shutil.rmtree')
    def test_incremental_cleanup_continues_on_error(self, mock_rmtree, mock_download,
                                                  mock_exists, mock_makedirs):
        """Tests that incremental backup cleanup continues even if some operations fail."""
        base_prefix = 'backups/full/20230101_000000'
        mock_exists.return_value = True
        mock_download.return_value = False
        mock_rmtree.side_effect = OSError("Failed to remove directory")
        
        result = backup.perform_incremental_backup(self.config, base_prefix)
        
        self.assertFalse(result)
        mock_download.assert_called_once()
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
        
    @patch('backupmate.backup.sqlite3.connect')
    def test_list_backups_from_db_success(self, mock_connect):
        """Tests successful backup listing from database."""
        # Mock cursor and connection
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock query results
        expected_backups = [
            ('full', 'backups/full/20230101', '2023-01-01 00:00:00', 'success'),
            ('incremental', 'backups/inc/20230102', '2023-01-02 00:00:00', 'success')
        ]
        mock_cursor.fetchall.return_value = expected_backups
        
        # Test normal list
        result = backup.list_backups_from_db(self.config)
        self.assertEqual(result, expected_backups)
        
        # Test JSON output
        result_json = backup.list_backups_from_db(self.config, output_json=True)
        self.assertIsInstance(result_json, str)
        
    @patch('backupmate.backup.sqlite3.connect')
    def test_record_backup_metadata_success(self, mock_connect):
        """Tests successful backup metadata recording."""
        # Mock cursor and connection
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        result = backup.record_backup_metadata(
            self.config,
            'full',
            'backups/full/20230101'
        )
        
        self.assertTrue(result)
        # Verify both SQL calls (CREATE TABLE and INSERT)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_conn.commit.assert_called()

if __name__ == '__main__':
    unittest.main()
