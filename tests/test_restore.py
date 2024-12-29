import unittest
from unittest.mock import patch, MagicMock
import subprocess
import os
from backupmate import restore

class TestRestore(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            'S3_BUCKET_NAME': 'test-bucket',
            'LOCAL_TEMP_DIR': '/tmp/test_restore',
            'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup',
            'MARIADB_DATADIR': '/var/lib/mysql',
            'INNODB_DATA_HOME_DIR': '/var/lib/mysql/innodb',
            'INNODB_LOG_GROUP_HOME_DIR': '/var/lib/mysql/innodb',
            'IS_TEST': True
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
        prepared_folder = "/tmp/test_restore/temp/backup"
        mock_download_prepare.return_value = prepared_folder
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
        mock_restore_backup.assert_called_once_with(prepared_folder, 
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

    @patch('backupmate.s3.download_file')
    @patch('backupmate.mariadb.prepare_backup')
    @patch('backupmate.utils.decompress_archive')
    @patch('os.makedirs')
    @patch('os.path.exists')
    @patch('os.path.join')
    def test_download_and_prepare_backup_success(self, mock_join, mock_exists, mock_makedirs, 
                                               mock_decompress, mock_prepare_backup, mock_download):
        """Tests successful backup download and preparation."""
        # Configure mocks
        mock_download.return_value = True
        mock_exists.return_value = True
        mock_decompress.return_value = "/tmp/test_restore/temp/backup"
        mock_prepare_backup.return_value = True
        mock_join.side_effect = lambda *args: '/'.join(args)

        result = restore.download_and_prepare_backup(self.backup_prefix, 
                                                   self.config['LOCAL_TEMP_DIR'], 
                                                   self.config)

        self.assertEqual(result, "/tmp/test_restore/temp/backup")
        # Verify temp directory creation
        mock_makedirs.assert_any_call('/tmp/test_restore/temp', exist_ok=True)
        # Verify download to temp directory
        mock_download.assert_called_once_with(self.config['S3_BUCKET_NAME'], 
                                            self.backup_prefix,
                                            '/tmp/test_restore/temp/01', 
                                            self.config)
        # Verify archive extraction
        mock_decompress.assert_called_once()
        # Verify backup preparation on extracted directory
        mock_prepare_backup.assert_called_once()

    @patch('backupmate.s3.download_file')
    @patch('os.makedirs')
    def test_download_and_prepare_backup_download_failure(self, mock_makedirs, mock_download):
        """Tests handling of S3 download failure."""
        mock_download.return_value = False

        result = restore.download_and_prepare_backup(self.backup_prefix, 
                                                   self.config['LOCAL_TEMP_DIR'], 
                                                   self.config)

        self.assertFalse(result)
        expected_temp_dir = os.path.join('/tmp/test_restore', 'temp')
        mock_makedirs.assert_called_once_with(expected_temp_dir, exist_ok=True)

    @patch('backupmate.s3.download_file')
    @patch('backupmate.utils.decompress_archive')
    @patch('os.makedirs')
    @patch('os.path.exists')
    def test_download_and_prepare_backup_no_tarfile(self, mock_exists, mock_makedirs, 
                                                  mock_decompress, mock_download):
        """Tests handling of missing tar.gz file."""
        mock_download.return_value = True
        mock_exists.return_value = False

        result = restore.download_and_prepare_backup(self.backup_prefix, 
                                                   self.config['LOCAL_TEMP_DIR'], 
                                                   self.config)

        self.assertFalse(result)
        mock_decompress.assert_not_called()

    @patch('backupmate.s3.download_file')
    @patch('backupmate.utils.decompress_archive')
    @patch('os.makedirs')
    @patch('os.path.exists')
    def test_download_and_prepare_backup_extraction_failure(self, mock_exists, mock_makedirs, 
                                                          mock_decompress, mock_download):
        """Tests handling of archive extraction failure."""
        mock_download.return_value = True
        mock_exists.return_value = True
        mock_decompress.return_value = False

        result = restore.download_and_prepare_backup(self.backup_prefix, 
                                                   self.config['LOCAL_TEMP_DIR'], 
                                                   self.config)

        self.assertFalse(result)

    @patch('subprocess.run')
    @patch('backupmate.config.load_config')
    def test_stop_mariadb_server_custom_command_success(self, mock_load_config, mock_run):
        """Tests successful MariaDB server stop using custom command."""
        mock_run.return_value = MagicMock(returncode=0)
        mock_load_config.return_value = {'MYSQL_STOP_COMMAND': 'kill -9 $(cat /var/run/mysql.pid)'}
        
        result = restore.stop_mariadb_server()
        
        self.assertTrue(result)
        mock_run.assert_called_once_with('kill -9 $(cat /var/run/mysql.pid)', 
                                       shell=True, 
                                       check=True)

    @patch('subprocess.run')
    @patch('backupmate.config.load_config')
    def test_stop_mariadb_server_systemctl_success(self, mock_load_config, mock_run):
        """Tests successful MariaDB server stop using systemctl."""
        mock_run.return_value = MagicMock(returncode=0)
        mock_load_config.return_value = {}  # No custom command
        
        result = restore.stop_mariadb_server()
        
        self.assertTrue(result)
        mock_run.assert_called_once_with(['systemctl', 'stop', 'mariadb'], 
                                       check=True, 
                                       capture_output=True)

    @patch('subprocess.run')
    @patch('backupmate.config.load_config')
    def test_stop_mariadb_server_failure(self, mock_load_config, mock_run):
        """Tests handling of MariaDB server stop failure."""
        mock_run.side_effect = subprocess.CalledProcessError(1, 'cmd')
        mock_load_config.return_value = {}
        
        result = restore.stop_mariadb_server()
        
        self.assertFalse(result)

    @patch('subprocess.run')
    @patch('backupmate.config.load_config')
    def test_start_mariadb_server_custom_command_success(self, mock_load_config, mock_run):
        """Tests successful MariaDB server start using custom command."""
        mock_run.return_value = MagicMock(returncode=0)
        mock_load_config.return_value = {'MYSQL_START_COMMAND': 'mysqld --datadir=/var/lib/mysql'}
        
        result = restore.start_mariadb_server()
        
        self.assertTrue(result)
        mock_run.assert_called_once_with('mysqld --datadir=/var/lib/mysql', 
                                       shell=True, 
                                       check=True)

    @patch('subprocess.run')
    @patch('backupmate.config.load_config')
    def test_start_mariadb_server_systemctl_success(self, mock_load_config, mock_run):
        """Tests successful MariaDB server start using systemctl."""
        mock_run.return_value = MagicMock(returncode=0)
        mock_load_config.return_value = {}  # No custom command
        
        result = restore.start_mariadb_server()
        
        self.assertTrue(result)
        mock_run.assert_called_once_with(['systemctl', 'start', 'mariadb'], 
                                       check=True, 
                                       capture_output=True)

    @patch('subprocess.run')
    @patch('backupmate.config.load_config')
    def test_start_mariadb_server_failure(self, mock_load_config, mock_run):
        """Tests handling of MariaDB server start failure."""
        mock_run.side_effect = subprocess.CalledProcessError(1, 'cmd')
        mock_load_config.return_value = {}
        
        result = restore.start_mariadb_server()
        
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()
