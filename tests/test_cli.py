import unittest
from unittest.mock import patch, MagicMock
import argparse
import json
from io import StringIO
import sys

from backupmate.cli import main, handle_backup, handle_restore, handle_list

class TestCLI(unittest.TestCase):
    def setUp(self):
        self.config = {
            'LOCAL_TEMP_DIR': '/tmp/backupmate',
            'S3_BUCKET_NAME': 'test-bucket',
            'FULL_BACKUP_PREFIX': 'backupmate/full/',
            'INCREMENTAL_BACKUP_PREFIX': 'backupmate/incremental/'
        }
        self.logger = MagicMock()

    @patch('backupmate.cli.load_config')
    @patch('backupmate.cli.validate_config')
    @patch('backupmate.cli.setup_logger')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_backup(self, mock_args, mock_logger, mock_validate, mock_load):
        # Setup mocks
        mock_args.return_value = argparse.Namespace(
            command='backup',
            full=True
        )
        mock_load.return_value = self.config
        mock_validate.return_value = True
        mock_logger.return_value = self.logger

        # Capture stdout
        stdout = StringIO()
        sys.stdout = stdout

        try:
            with patch('backupmate.cli.perform_full_backup') as mock_backup:
                mock_backup.return_value = True
                result = main()
                self.assertEqual(result, 0)
                mock_backup.assert_called_once()
        finally:
            sys.stdout = sys.__stdout__

    @patch('backupmate.cli.load_config')
    @patch('backupmate.cli.validate_config')
    @patch('backupmate.cli.setup_logger')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_restore(self, mock_args, mock_logger, mock_validate, mock_load):
        # Setup mocks
        mock_args.return_value = argparse.Namespace(
            command='restore',
            backup_id='test-backup',
            latest_full=False,
            latest_incremental=False,
            copy_back=True,
            move_back=False,
            json=False
        )
        mock_load.return_value = self.config
        mock_validate.return_value = True
        mock_logger.return_value = self.logger

        with patch('backupmate.cli.restore_specific_backup') as mock_restore:
            mock_restore.return_value = True
            result = main()
            self.assertEqual(result, 0)
            mock_restore.assert_called_once()

    @patch('backupmate.cli.load_config')
    @patch('backupmate.cli.validate_config')
    @patch('backupmate.cli.setup_logger')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_list(self, mock_args, mock_logger, mock_validate, mock_load):
        # Setup mocks
        mock_args.return_value = argparse.Namespace(
            command='list',
            json=True
        )
        mock_load.return_value = self.config
        mock_validate.return_value = True
        mock_logger.return_value = self.logger

        # Capture stdout
        stdout = StringIO()
        sys.stdout = stdout

        try:
            with patch('backupmate.s3.list_objects') as mock_list:
                mock_list.return_value = ['backup1', 'backup2']
                result = main()
                self.assertEqual(result, 0)
                self.assertEqual(mock_list.call_count, 2)  # Called for both full and incremental
                
                # Verify JSON output
                output = json.loads(stdout.getvalue())
                self.assertTrue(isinstance(output, list))
                # Should have 4 backups total (2 full + 2 incremental)
                self.assertEqual(len(output), 4)
                # Verify we have both types
                types = {b['type'] for b in output}
                self.assertEqual(types, {'full', 'incremental'})
                # Verify structure of backup objects
                for backup in output:
                    self.assertIn('type', backup)
                    self.assertIn('id', backup)
        finally:
            sys.stdout = sys.__stdout__

    def test_handle_backup_full(self):
        args = argparse.Namespace(full=True)
        
        with patch('backupmate.cli.perform_full_backup') as mock_backup:
            mock_backup.return_value = True
            result = handle_backup(args, self.config, self.logger)
            self.assertTrue(result)
            mock_backup.assert_called_once_with(self.config)

    @patch('backupmate.s3.get_latest_backup_prefix')
    def test_handle_backup_incremental(self, mock_get_latest):
        args = argparse.Namespace(full=False)
        mock_get_latest.return_value = 'backups/full/latest'
        
        with patch('backupmate.cli.perform_incremental_backup') as mock_backup:
            mock_backup.return_value = True
            result = handle_backup(args, self.config, self.logger)
            self.assertTrue(result)
            mock_backup.assert_called_once_with(self.config, 'backups/full/latest')

    def test_handle_restore_validation(self):
        # Test multiple restore options specified
        args = argparse.Namespace(
            backup_id='test',
            latest_full=True,
            latest_incremental=False,
            copy_back=True,
            move_back=False
        )
        result = handle_restore(args, self.config, self.logger)
        self.assertFalse(result)

        # Test conflicting restore methods
        args = argparse.Namespace(
            backup_id='test',
            latest_full=False,
            latest_incremental=False,
            copy_back=True,
            move_back=True
        )
        result = handle_restore(args, self.config, self.logger)
        self.assertFalse(result)

    def test_handle_restore_success_with_backup_id(self):
        """Test restore with specific backup ID"""
        args = argparse.Namespace(
            backup_id='test',
            latest_full=False,
            latest_incremental=False,
            copy_back=True,
            move_back=False
        )
        
        with patch('backupmate.cli.restore_specific_backup') as mock_restore:
            mock_restore.return_value = True
            result = handle_restore(args, self.config, self.logger)
            self.assertTrue(result)
            mock_restore.assert_called_once_with(
                backup_identifier='test',
                restore_method='copy-back',
                config=self.config
            )

    @patch('backupmate.s3.list_objects')
    def test_handle_restore_success_with_latest_full(self, mock_list_objects):
        """Test restore with --latest-full flag"""
        args = argparse.Namespace(
            backup_id=None,
            latest_full=True,
            latest_incremental=False,
            copy_back=True,
            move_back=False
        )
        
        mock_list_objects.return_value = ['backup1', 'backup2', 'backup3']
        
        with patch('backupmate.cli.restore_specific_backup') as mock_restore:
            mock_restore.return_value = True
            result = handle_restore(args, self.config, self.logger)
            self.assertTrue(result)
            mock_restore.assert_called_once_with(
                backup_identifier='backup3',  # Should use latest backup
                restore_method='copy-back',
                config=self.config
            )
            mock_list_objects.assert_called_once_with(
                self.config['S3_BUCKET_NAME'],
                self.config['FULL_BACKUP_PREFIX'],
                self.config
            )

    @patch('backupmate.s3.list_objects')
    def test_handle_restore_success_with_latest_incremental(self, mock_list_objects):
        """Test restore with --latest-incremental flag"""
        args = argparse.Namespace(
            backup_id=None,
            latest_full=False,
            latest_incremental=True,
            copy_back=True,
            move_back=False
        )
        
        mock_list_objects.return_value = ['backup1', 'backup2', 'backup3']
        
        with patch('backupmate.cli.restore_specific_backup') as mock_restore:
            mock_restore.return_value = True
            result = handle_restore(args, self.config, self.logger)
            self.assertTrue(result)
            mock_restore.assert_called_once_with(
                backup_identifier='backup3',  # Should use latest backup
                restore_method='copy-back',
                config=self.config
            )
            mock_list_objects.assert_called_once_with(
                self.config['S3_BUCKET_NAME'],
                self.config['INCREMENTAL_BACKUP_PREFIX'],
                self.config
            )

    @patch('backupmate.s3.list_objects')
    def test_handle_restore_no_backups_found(self, mock_list_objects):
        """Test restore when no backups are found"""
        args = argparse.Namespace(
            backup_id=None,
            latest_full=True,
            latest_incremental=False,
            copy_back=True,
            move_back=False
        )
        
        mock_list_objects.return_value = []
        
        with patch('backupmate.cli.restore_specific_backup') as mock_restore:
            result = handle_restore(args, self.config, self.logger)
            self.assertFalse(result)
            mock_restore.assert_not_called()

    def test_handle_list(self):
        args = argparse.Namespace(json=False)
        
        with patch('backupmate.s3.list_objects') as mock_list:
            mock_list.return_value = ['backup1', 'backup2']
            
            # Capture stdout
            stdout = StringIO()
            sys.stdout = stdout
            
            try:
                result = handle_list(args, self.config, self.logger)
                self.assertTrue(result)
                self.assertEqual(mock_list.call_count, 2)  # Called for both full and incremental
                
                # Verify text output format
                output = stdout.getvalue()
                self.assertIn('Full Backups:', output)
                self.assertIn('Incremental Backups:', output)
                # Each backup should appear in output
                for backup_id in ['backup1', 'backup2']:
                    self.assertIn(backup_id, output)
            finally:
                sys.stdout = sys.__stdout__

if __name__ == '__main__':
    unittest.main()
