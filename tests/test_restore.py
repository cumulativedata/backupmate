import unittest
from unittest.mock import patch, MagicMock
import subprocess
from backupmate import restore

class TestRestore(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            'S3_BUCKET_NAME': 'test-bucket',
            'LOCAL_TEMP_DIR': '/tmp/test_restore',
            'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup'
        }
        self.backup_prefix = 'backups/2023/01/01/'

    @patch('backupmate.restore.download_and_prepare_backup')
    @patch('backupmate.restore.stop_mariadb_server')
    @patch('backupmate.restore.start_mariadb_server')
    @patch('backupmate.mariadb.restore_backup')
    def test_restore_specific_backup_success(self, mock_restore_backup, mock_start_server, 
                                          mock_stop_server, mock_download_prepare):
        """Tests successful backup restoration."""
        # Configure mocks
        mock_download_prepare.return_value = True
        mock_stop_server.return_value = True
        mock_restore_backup.return_value = True
        mock_start_server.return_value = True

        # Test
        result = restore.restore_specific_backup(self.backup_prefix, 'copy-back', self.config)

        # Verify
        self.assertTrue(result)
        mock_download_prepare.assert_called_once_with(self.backup_prefix, 
                                                    self.config['LOCAL_TEMP_DIR'], 
                                                    self.config)
        mock_stop_server.assert_called_once()
        mock_restore_backup.assert_called_once_with(self.config['LOCAL_TEMP_DIR'], 
                                                  self.config, 
                                                  method='copy-back')
        mock_start_server.assert_called_once()

    def test_restore_specific_backup_invalid_method(self):
        """Tests restoration with invalid restore method."""
        result = restore.restore_specific_backup(self.backup_prefix, 'invalid-method', self.config)
        self.assertFalse(result)

    @patch('backupmate.restore.download_and_prepare_backup')
    @patch('backupmate.restore.stop_mariadb_server')
    @patch('backupmate.restore.start_mariadb_server')
    def test_restore_specific_backup_download_failure(self, mock_start_server, 
                                                    mock_stop_server, mock_download_prepare):
        """Tests handling of download failure during restoration."""
        mock_download_prepare.return_value = False
        
        result = restore.restore_specific_backup(self.backup_prefix, 'copy-back', self.config)
        
        self.assertFalse(result)
        mock_stop_server.assert_not_called()
        mock_start_server.assert_not_called()

    @patch('backupmate.s3.download_directory')
    @patch('backupmate.mariadb.prepare_backup')
    @patch('backupmate.utils.decompress_archive')
    @patch('os.makedirs')
    @patch('os.listdir')
    @patch('os.path.join')
    def test_download_and_prepare_backup_success(self, mock_join, mock_listdir, mock_makedirs, 
                                               mock_decompress, mock_prepare_backup, mock_download):
        """Tests successful backup download and preparation."""
        # Configure mocks
        mock_download.return_value = True
        mock_decompress.return_value = True
        mock_prepare_backup.return_value = True
        mock_listdir.side_effect = [
            ['backup.tar.gz'],  # First call for tar files
            ['backup_dir']      # Second call for extracted dirs
        ]
        mock_join.side_effect = lambda *args: '/'.join(args)

        result = restore.download_and_prepare_backup(self.backup_prefix, 
                                                   self.config['LOCAL_TEMP_DIR'], 
                                                   self.config)

        self.assertTrue(result)
        # Verify temp directory creation
        mock_makedirs.assert_any_call('/tmp/test_restore/temp', exist_ok=True)
        # Verify download to temp directory
        mock_download.assert_called_once_with(self.config['S3_BUCKET_NAME'], 
                                            self.backup_prefix,
                                            '/tmp/test_restore/temp', 
                                            self.config)
        # Verify archive extraction
        mock_decompress.assert_called_once()
        # Verify backup preparation on extracted directory
        mock_prepare_backup.assert_called_once()

    @patch('backupmate.s3.download_directory')
    @patch('os.makedirs')
    def test_download_and_prepare_backup_download_failure(self, mock_makedirs, mock_download):
        """Tests handling of S3 download failure."""
        mock_download.return_value = False

        result = restore.download_and_prepare_backup(self.backup_prefix, 
                                                   self.config['LOCAL_TEMP_DIR'], 
                                                   self.config)

        self.assertFalse(result)
        mock_makedirs.assert_called_once()  # Should still try to create temp dir

    @patch('backupmate.s3.download_directory')
    @patch('backupmate.utils.decompress_archive')
    @patch('os.makedirs')
    @patch('os.listdir')
    def test_download_and_prepare_backup_no_tarfile(self, mock_listdir, mock_makedirs, 
                                                  mock_decompress, mock_download):
        """Tests handling of missing tar.gz file."""
        mock_download.return_value = True
        mock_listdir.return_value = ['not_a_tarfile.txt']

        result = restore.download_and_prepare_backup(self.backup_prefix, 
                                                   self.config['LOCAL_TEMP_DIR'], 
                                                   self.config)

        self.assertFalse(result)
        mock_decompress.assert_not_called()

    @patch('backupmate.s3.download_directory')
    @patch('backupmate.utils.decompress_archive')
    @patch('os.makedirs')
    @patch('os.listdir')
    def test_download_and_prepare_backup_extraction_failure(self, mock_listdir, mock_makedirs, 
                                                          mock_decompress, mock_download):
        """Tests handling of archive extraction failure."""
        mock_download.return_value = True
        mock_listdir.return_value = ['backup.tar.gz']
        mock_decompress.return_value = False

        result = restore.download_and_prepare_backup(self.backup_prefix, 
                                                   self.config['LOCAL_TEMP_DIR'], 
                                                   self.config)

        self.assertFalse(result)

    @patch('subprocess.run')
    def test_stop_mariadb_server_success(self, mock_run):
        """Tests successful MariaDB server stop."""
        mock_run.return_value = MagicMock(returncode=0)
        
        result = restore.stop_mariadb_server()
        
        self.assertTrue(result)
        mock_run.assert_called_once_with(['systemctl', 'stop', 'mariadb'], 
                                       check=True, 
                                       capture_output=True)

    @patch('subprocess.run')
    def test_stop_mariadb_server_failure(self, mock_run):
        """Tests handling of MariaDB server stop failure."""
        mock_run.side_effect = subprocess.CalledProcessError(1, 'cmd')
        
        result = restore.stop_mariadb_server()
        
        self.assertFalse(result)

    @patch('subprocess.run')
    def test_start_mariadb_server_success(self, mock_run):
        """Tests successful MariaDB server start."""
        mock_run.return_value = MagicMock(returncode=0)
        
        result = restore.start_mariadb_server()
        
        self.assertTrue(result)
        mock_run.assert_called_once_with(['systemctl', 'start', 'mariadb'], 
                                       check=True, 
                                       capture_output=True)

    @patch('subprocess.run')
    def test_start_mariadb_server_failure(self, mock_run):
        """Tests handling of MariaDB server start failure."""
        mock_run.side_effect = subprocess.CalledProcessError(1, 'cmd')
        
        result = restore.start_mariadb_server()
        
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()
