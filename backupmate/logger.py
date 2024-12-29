import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

class JsonFormatter(logging.Formatter):
    """Custom formatter that outputs log records in JSON format."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON string."""
        # Base log data
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        
        # Add extra data if present
        if hasattr(record, "data") and record.data is not None:
            log_data["data"] = record.data
            
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
            
        return json.dumps(log_data)

def setup_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    """
    Set up and return a logger instance that outputs logs in JSON format.
    
    Args:
        name: The name of the logger
        log_file: Optional path to a log file. If not provided, logs to console only.
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Create JSON formatter
    formatter = JsonFormatter()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler if log_file is specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def log_info(logger: logging.Logger, message: str, data: Optional[Dict[str, Any]] = None) -> None:
    """
    Log an informational message with optional additional data.
    
    Args:
        logger: The logger instance to use
        message: The log message
        data: Optional dictionary of additional data to include in the log
    """
    extra = {"data": data} if data else {}
    logger.info(message, extra=extra)

def log_error(logger: logging.Logger, message: str, data: Optional[Dict[str, Any]] = None, exc_info: bool = True) -> None:
    """
    Log an error message with optional additional data and exception info.
    
    Args:
        logger: The logger instance to use
        message: The error message
        data: Optional dictionary of additional data to include in the log
        exc_info: Whether to include exception information, defaults to True
    """
    extra = {"data": data} if data else {}
    logger.error(message, exc_info=exc_info, extra=extra)
