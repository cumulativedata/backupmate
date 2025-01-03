import json
import logging
import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from backupmate.logger import setup_logger, log_info, log_error, JsonFormatter

class TestLogger(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary log file for testing
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = os.path.join(self.temp_dir, "test.log")
        self.loggers = []  # Track loggers to clean up handlers
        
    def tearDown(self):
        """Clean up test fixtures."""
        # Close all logger handlers
        for logger in self.loggers:
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)
        
        # Clean up temporary files
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
        os.rmdir(self.temp_dir)
            
    def test_json_formatter(self):
        """Test that the JsonFormatter correctly formats log records."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        self.assertIn("timestamp", log_data)
        self.assertEqual(log_data["level"], "INFO")
        self.assertEqual(log_data["message"], "Test message")
        
    def test_json_formatter_with_data(self):
        """Test JSON formatting with additional data."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test with data",
            args=(),
            exc_info=None
        )
        record.data = {"key": "value"}
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        self.assertEqual(log_data["data"], {"key": "value"})
        
    def test_setup_logger_console(self):
        """Test logger setup with console output only."""
        logger = setup_logger("test_console")
        self.loggers.append(logger)
        
        self.assertEqual(logger.level, logging.INFO)
        self.assertEqual(len(logger.handlers), 1)
        self.assertIsInstance(logger.handlers[0], logging.StreamHandler)
        self.assertFalse(logger.propagate, "Logger should not propagate to avoid duplicate logs")
        
    def test_setup_logger_with_file(self):
        """Test logger setup with both console and file output."""
        logger = setup_logger("test_file", self.log_file)
        self.loggers.append(logger)
        
        self.assertEqual(len(logger.handlers), 2)
        self.assertIsInstance(logger.handlers[0], logging.StreamHandler)
        self.assertIsInstance(logger.handlers[1], logging.FileHandler)
        self.assertFalse(logger.propagate, "Logger should not propagate to avoid duplicate logs")

    def test_setup_logger_clears_existing_handlers(self):
        """Test that setup_logger clears any existing handlers."""
        # Create logger with initial handler
        logger_name = "test_handlers"
        initial_logger = logging.getLogger(logger_name)
        initial_handler = logging.StreamHandler()
        initial_logger.addHandler(initial_handler)
        self.assertEqual(len(initial_logger.handlers), 1)
        
        # Setup logger with same name
        new_logger = setup_logger(logger_name)
        self.loggers.append(new_logger)
        
        # Verify only one handler exists and it's the new one
        self.assertEqual(len(new_logger.handlers), 1)
        self.assertNotEqual(new_logger.handlers[0], initial_handler)
        
    def test_log_info(self):
        """Test info level logging."""
        from io import StringIO
        log_output = StringIO()
        logger = setup_logger("test_info")
        self.loggers.append(logger)
        
        # Replace the StreamHandler with our StringIO handler
        logger.handlers = []
        handler = logging.StreamHandler(log_output)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        
        test_data = {"status": "success"}
        log_info(logger, "Test info message", test_data)
        
        # Get the log output and parse it
        log_content = log_output.getvalue()
        log_data = json.loads(log_content)
        
        self.assertEqual(log_data["level"], "INFO")
        self.assertEqual(log_data["message"], "Test info message")
        self.assertEqual(log_data["data"], test_data)
        
    def test_log_error(self):
        """Test error level logging."""
        from io import StringIO
        log_output = StringIO()
        logger = setup_logger("test_error")
        self.loggers.append(logger)
        
        # Replace the StreamHandler with our StringIO handler
        logger.handlers = []
        handler = logging.StreamHandler(log_output)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        
        test_data = {"status": "failed"}
        try:
            raise ValueError("Test error")
        except ValueError:
            log_error(logger, "Test error message", test_data)
        
        # Get the log output and parse it
        log_content = log_output.getvalue()
        log_data = json.loads(log_content)
        
        self.assertEqual(log_data["level"], "ERROR")
        self.assertEqual(log_data["message"], "Test error message")
        self.assertEqual(log_data["data"], test_data)
        self.assertIn("ValueError: Test error", log_data["exception"])
        
    def test_log_to_file(self):
        """Test that logs are correctly written to file."""
        logger = setup_logger("test_file_output", self.log_file)
        self.loggers.append(logger)
        test_message = "Test file logging"
        test_data = {"key": "value"}
        
        log_info(logger, test_message, test_data)
        
        # Close handlers to ensure file is written
        for handler in logger.handlers:
            handler.close()
            
        # Verify the log file exists and contains the message
        self.assertTrue(os.path.exists(self.log_file))
        with open(self.log_file, 'r') as f:
            log_content = f.read()
            log_data = json.loads(log_content)
            self.assertEqual(log_data["message"], test_message)
            self.assertEqual(log_data["data"], test_data)

if __name__ == '__main__':
    unittest.main()
