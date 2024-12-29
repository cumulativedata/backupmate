import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
from botocore.exceptions import ClientError
from backupmate import s3

class TestS3(unittest.TestCase):
    def setUp(self):
        self.config = {
            'AWS_ACCESS_KEY_ID': 'test_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret',
            'AWS_REGION': 'us-east-1'
        }
        self.s3_bucket = 'test-bucket'
        self.s3_prefix = 'backups/'
        self.local_path = '/tmp/backups'

    @patch('backupmate.s3.boto3.client')
    @patch('backupmate.s3.os.walk')
    @patch('backupmate.s3.os.path.exists')
    def test_upload_directory_success(self, mock_exists, mock_walk, mock_boto3_client):
        """Tests successful S3 directory upload."""
        # Setup mocks
        mock_exists.return_value = True
        mock_walk.return_value = [
            ('/tmp/backups', [], ['file1.txt', 'file2.txt']),
            ('/tmp/backups/subdir', [], ['file3.txt'])
        ]
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3

        # Execute test
        result = s3.upload_directory(self.local_path, self.s3_bucket, self.s3_prefix, self.config)

        # Verify
        self.assertTrue(result)
        self.assertEqual(mock_s3.upload_file.call_count, 3)
        mock_boto3_client.assert_called_once_with(
            's3',
            aws_access_key_id='test_key',
            aws_secret_access_key='test_secret',
            region_name='us-east-1'
        )

    @patch('backupmate.s3.boto3.client')
    @patch('backupmate.s3.os.path.exists')
    def test_upload_directory_nonexistent_dir(self, mock_exists, mock_boto3_client):
        """Tests upload with non-existent directory."""
        mock_exists.return_value = False
        result = s3.upload_directory(self.local_path, self.s3_bucket, self.s3_prefix, self.config)
        self.assertFalse(result)
        mock_boto3_client.assert_not_called()

    @patch('backupmate.s3.boto3.client')
    @patch('backupmate.s3.os.walk')
    @patch('backupmate.s3.os.path.exists')
    def test_upload_directory_client_error(self, mock_exists, mock_walk, mock_boto3_client):
        """Tests error handling during S3 upload."""
        mock_exists.return_value = True
        mock_walk.return_value = [
            ('/tmp/backups', [], ['file1.txt'])
        ]
        mock_s3 = MagicMock()
        mock_s3.upload_file.side_effect = ClientError(
            {'Error': {'Code': 'TestException', 'Message': 'Test error'}},
            'upload_file'
        )
        mock_boto3_client.return_value = mock_s3

        result = s3.upload_directory(self.local_path, self.s3_bucket, self.s3_prefix, self.config)
        self.assertFalse(result)

    @patch('backupmate.s3.boto3.client')
    @patch('backupmate.s3.os.makedirs')
    def test_download_directory_success(self, mock_makedirs, mock_boto3_client):
        """Tests successful S3 directory download."""
        # Setup mock S3 client
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [{
            'Contents': [
                {'Key': 'backups/file1.txt'},
                {'Key': 'backups/subdir/file2.txt'}
            ]
        }]
        mock_boto3_client.return_value = mock_s3

        # Execute test
        result = s3.download_directory(self.s3_bucket, self.s3_prefix, self.local_path, self.config)

        # Verify
        self.assertTrue(result)
        self.assertEqual(mock_s3.download_file.call_count, 2)
        mock_makedirs.assert_called()

    @patch('backupmate.s3.boto3.client')
    def test_download_directory_client_error(self, mock_boto3_client):
        """Tests error handling during S3 download."""
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.side_effect = ClientError(
            {'Error': {'Code': 'TestException', 'Message': 'Test error'}},
            'list_objects_v2'
        )
        mock_boto3_client.return_value = mock_s3

        result = s3.download_directory(self.s3_bucket, self.s3_prefix, self.local_path, self.config)
        self.assertFalse(result)

    @patch('backupmate.s3.boto3.client')
    def test_list_objects_success(self, mock_boto3_client):
        """Tests listing objects in an S3 bucket."""
        # Setup mock response
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [{
            'Contents': [
                {'Key': 'backups/file1.txt'},
                {'Key': 'backups/file2.txt'}
            ]
        }]
        mock_boto3_client.return_value = mock_s3

        # Execute test
        result = s3.list_objects(self.s3_bucket, self.s3_prefix, self.config)

        # Verify
        self.assertEqual(len(result), 2)
        self.assertIn('backups/file1.txt', result)
        self.assertIn('backups/file2.txt', result)

    @patch('backupmate.s3.boto3.client')
    def test_list_objects_empty_response(self, mock_boto3_client):
        """Tests listing objects with empty response."""
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [{}]
        mock_boto3_client.return_value = mock_s3

        result = s3.list_objects(self.s3_bucket, self.s3_prefix, self.config)
        self.assertEqual(result, [])

    @patch('backupmate.s3.list_objects')
    def test_get_latest_backup_prefix_success(self, mock_list_objects):
        """Tests retrieving the latest backup prefix from S3."""
        mock_list_objects.return_value = [
            'backups/2023-01-01/backup1.tar.gz',
            'backups/2023-01-02/backup2.tar.gz',
            'backups/2023-01-03/backup3.tar.gz'
        ]

        result = s3.get_latest_backup_prefix(self.s3_bucket, self.s3_prefix, self.config)
        self.assertEqual(result, 'backups/2023-01-03/backup3.tar.gz')

    @patch('backupmate.s3.list_objects')
    def test_get_latest_backup_prefix_no_backups(self, mock_list_objects):
        """Tests retrieving latest backup prefix with no backups."""
        mock_list_objects.return_value = []

        result = s3.get_latest_backup_prefix(self.s3_bucket, self.s3_prefix, self.config)
        self.assertIsNone(result)

    @patch('backupmate.s3.list_objects')
    def test_get_latest_backup_prefix_error(self, mock_list_objects):
        """Tests error handling in get_latest_backup_prefix."""
        mock_list_objects.side_effect = Exception("Test error")

        result = s3.get_latest_backup_prefix(self.s3_bucket, self.s3_prefix, self.config)
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()
