import unittest
from unittest.mock import patch, call
import subprocess
from backupmate import mariadb

class TestMariadb(unittest.TestCase):
    @patch('backupmate.mariadb.subprocess.run')
    def test_take_full_backup_success(self, mock_run):
        mock_run.return_value.check_returncode.return_value = None
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup', 'DB_HOST': 'localhost', 'DB_PORT': '3306', 'DB_USER': 'test', 'DB_PASSWORD': 'password'}
        result = mariadb.take_full_backup('backup_dir', config)
        self.assertTrue(result)
        mock_run.assert_called_once_with(
            ['/usr/bin/mariabackup', '--backup', '--target-dir=backup_dir', '--host=localhost', '--port=3306', '--user=test', '--password=password'],
            check=True,
            capture_output=True
        )

    @patch('backupmate.mariadb.subprocess.run')
    def test_take_full_backup_failure_calledprocesserror(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, 'command')
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup', 'DB_HOST': 'localhost', 'DB_PORT': '3306', 'DB_USER': 'test', 'DB_PASSWORD': 'password'}
        result = mariadb.take_full_backup('backup_dir', config)
        self.assertFalse(result)

    @patch('backupmate.mariadb.subprocess.run')
    def test_take_full_backup_failure_filenotfounderror(self, mock_run):
        mock_run.side_effect = FileNotFoundError('mariabackup not found')
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup', 'DB_HOST': 'localhost', 'DB_PORT': '3306', 'DB_USER': 'test', 'DB_PASSWORD': 'password'}
        result = mariadb.take_full_backup('backup_dir', config)
        self.assertFalse(result)

    @patch('backupmate.mariadb.subprocess.run')
    def test_take_incremental_backup_success(self, mock_run):
        mock_run.return_value.check_returncode.return_value = None
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup', 'DB_HOST': 'localhost', 'DB_PORT': '3306', 'DB_USER': 'test', 'DB_PASSWORD': 'password'}
        result = mariadb.take_incremental_backup('backup_dir', 'basedir', config)
        self.assertTrue(result)
        mock_run.assert_called_once_with(
            ['/usr/bin/mariabackup', '--backup', '--target-dir=backup_dir', '--incremental-basedir=basedir', '--host=localhost', '--port=3306', '--user=test', '--password=password'],
            check=True,
            capture_output=True
        )

    @patch('backupmate.mariadb.subprocess.run')
    def test_take_incremental_backup_failure_calledprocesserror(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, 'command')
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup', 'DB_HOST': 'localhost', 'DB_PORT': '3306', 'DB_USER': 'test', 'DB_PASSWORD': 'password'}
        result = mariadb.take_incremental_backup('backup_dir', 'basedir', config)
        self.assertFalse(result)

    @patch('backupmate.mariadb.subprocess.run')
    def test_take_incremental_backup_failure_filenotfounderror(self, mock_run):
        mock_run.side_effect = FileNotFoundError('mariabackup not found')
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup', 'DB_HOST': 'localhost', 'DB_PORT': '3306', 'DB_USER': 'test', 'DB_PASSWORD': 'password'}
        result = mariadb.take_incremental_backup('backup_dir', 'basedir', config)
        self.assertFalse(result)

    @patch('backupmate.mariadb.subprocess.run')
    def test_prepare_backup_success(self, mock_run):
        mock_run.return_value.check_returncode.return_value = None
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup'}
        result = mariadb.prepare_backup('backup_dir', config=config)
        self.assertTrue(result)
        mock_run.assert_called_once_with(
            ['/usr/bin/mariabackup', '--prepare', '--target-dir=backup_dir'],
            check=True,
            capture_output=True
        )

    @patch('backupmate.mariadb.subprocess.run')
    def test_prepare_backup_with_incrementals_success(self, mock_run):
        mock_run.return_value.check_returncode.return_value = None
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup'}
        incremental_dirs = ['inc1', 'inc2']
        result = mariadb.prepare_backup('backup_dir', incremental_dirs, config)
        self.assertTrue(result)
        mock_run.assert_called_once_with(
            ['/usr/bin/mariabackup', '--prepare', '--target-dir=backup_dir', '--incremental-dir=inc1', '--incremental-dir=inc2'],
            check=True,
            capture_output=True
        )

    @patch('backupmate.mariadb.subprocess.run')
    def test_prepare_backup_failure_calledprocesserror(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, 'command')
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup'}
        result = mariadb.prepare_backup('backup_dir', config=config)
        self.assertFalse(result)

    @patch('backupmate.mariadb.subprocess.run')
    def test_prepare_backup_failure_filenotfounderror(self, mock_run):
        mock_run.side_effect = FileNotFoundError('mariabackup not found')
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup'}
        result = mariadb.prepare_backup('backup_dir', config=config)
        self.assertFalse(result)

    @patch('backupmate.mariadb.subprocess.run')
    def test_restore_backup_copy_back_success(self, mock_run):
        mock_run.return_value.check_returncode.return_value = None
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup'}
        result = mariadb.restore_backup('backup_dir', config, method='copy-back')
        self.assertTrue(result)
        mock_run.assert_called_once_with(
            ['/usr/bin/mariabackup', '--copy-back', '--target-dir=backup_dir'],
            check=True,
            capture_output=True
        )

    @patch('backupmate.mariadb.subprocess.run')
    def test_restore_backup_move_back_success(self, mock_run):
        mock_run.return_value.check_returncode.return_value = None
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup'}
        result = mariadb.restore_backup('backup_dir', config, method='move-back')
        self.assertTrue(result)
        mock_run.assert_called_once_with(
            ['/usr/bin/mariabackup', '--move-back', '--target-dir=backup_dir'],
            check=True,
            capture_output=True
        )

    @patch('backupmate.mariadb.subprocess.run')
    def test_restore_backup_failure_calledprocesserror(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, 'command')
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup'}
        result = mariadb.restore_backup('backup_dir', config)
        self.assertFalse(result)

    @patch('backupmate.mariadb.subprocess.run')
    def test_restore_backup_failure_filenotfounderror(self, mock_run):
        mock_run.side_effect = FileNotFoundError('mariabackup not found')
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup'}
        result = mariadb.restore_backup('backup_dir', config)
        self.assertFalse(result)

    def test_restore_backup_invalid_method(self):
        config = {'MARIADB_BACKUP_PATH': '/usr/bin/mariabackup'}
        result = mariadb.restore_backup('backup_dir', config, method='invalid')
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()
