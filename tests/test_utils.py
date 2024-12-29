import unittest
from unittest.mock import patch, mock_open, MagicMock
import os
from pathlib import Path
import tarfile
import shutil
from backupmate.utils import compress_directory, decompress_archive, ensure_directory, clean_directory

class TestUtils(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_dir"
        self.test_archive = "test_archive.tar.gz"
        self.output_dir = "output_dir"

    @patch('pathlib.Path.is_dir')
    @patch('tarfile.open')
    @patch('pathlib.Path.mkdir')
    def test_compress_directory_success(self, mock_mkdir, mock_tarfile, mock_is_dir):
        mock_is_dir.return_value = True
        mock_tar = MagicMock()
        mock_tarfile.return_value.__enter__.return_value = mock_tar
        
        result = compress_directory(self.test_dir, self.test_archive)
        
        self.assertTrue(result)
        mock_mkdir.assert_called_once()
        mock_tar.add.assert_called_once()

    @patch('pathlib.Path.is_dir')
    def test_compress_directory_missing_source(self, mock_is_dir):
        mock_is_dir.return_value = False
        
        result = compress_directory(self.test_dir, self.test_archive)
        
        self.assertFalse(result)

    @patch('pathlib.Path.is_dir')
    @patch('tarfile.open')
    def test_compress_directory_permission_error(self, mock_tarfile, mock_is_dir):
        mock_is_dir.return_value = True
        mock_tarfile.side_effect = PermissionError()
        
        result = compress_directory(self.test_dir, self.test_archive)
        
        self.assertFalse(result)

    @patch('pathlib.Path.is_file')
    @patch('tarfile.open')
    @patch('pathlib.Path.mkdir')
    def test_decompress_archive_success(self, mock_mkdir, mock_tarfile, mock_is_file):
        mock_is_file.return_value = True
        mock_tar = MagicMock()
        mock_tar.getmembers.return_value = [MagicMock(name='safe/path')]
        mock_tarfile.return_value.__enter__.return_value = mock_tar
        
        result = decompress_archive(self.test_archive, self.output_dir)
        
        self.assertTrue(result)
        mock_mkdir.assert_called_once()
        mock_tar.extractall.assert_called_once()

    @patch('pathlib.Path.is_file')
    def test_decompress_archive_missing_archive(self, mock_is_file):
        mock_is_file.return_value = False
        
        result = decompress_archive(self.test_archive, self.output_dir)
        
        self.assertFalse(result)

    @patch('pathlib.Path.is_file')
    @patch('tarfile.open')
    def test_decompress_archive_suspicious_path(self, mock_tarfile, mock_is_file):
        mock_is_file.return_value = True
        mock_tar = MagicMock()
        mock_tar.getmembers.return_value = [MagicMock(name='/etc/passwd')]
        mock_tarfile.return_value.__enter__.return_value = mock_tar
        
        result = decompress_archive(self.test_archive, self.output_dir)
        
        self.assertFalse(result)
        mock_tar.extractall.assert_not_called()

    @patch('pathlib.Path.mkdir')
    def test_ensure_directory_success(self, mock_mkdir):
        result = ensure_directory(self.test_dir)
        
        self.assertTrue(result)
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    @patch('pathlib.Path.mkdir')
    def test_ensure_directory_permission_error(self, mock_mkdir):
        mock_mkdir.side_effect = PermissionError()
        
        result = ensure_directory(self.test_dir)
        
        self.assertFalse(result)

    @patch('pathlib.Path.is_dir')
    @patch('pathlib.Path.iterdir')
    @patch('pathlib.Path.unlink')
    @patch('shutil.rmtree')
    def test_clean_directory_success(self, mock_rmtree, mock_unlink, mock_iterdir, mock_is_dir):
        mock_is_dir.return_value = True
        file_mock = MagicMock()
        file_mock.is_file.return_value = True
        file_mock.is_dir.return_value = False
        dir_mock = MagicMock()
        dir_mock.is_file.return_value = False
        dir_mock.is_dir.return_value = True
        mock_iterdir.return_value = [file_mock, dir_mock]
        
        result = clean_directory(self.test_dir)
        
        self.assertTrue(result)
        mock_unlink.assert_called_once()
        mock_rmtree.assert_called_once()

    @patch('pathlib.Path.is_dir')
    def test_clean_directory_missing_directory(self, mock_is_dir):
        mock_is_dir.return_value = False
        
        result = clean_directory(self.test_dir)
        
        self.assertFalse(result)

    @patch('pathlib.Path.is_dir')
    @patch('pathlib.Path.iterdir')
    def test_clean_directory_permission_error(self, mock_iterdir, mock_is_dir):
        mock_is_dir.return_value = True
        mock_iterdir.side_effect = PermissionError()
        
        result = clean_directory(self.test_dir)
        
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()
