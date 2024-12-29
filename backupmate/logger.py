import logging
import json

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if hasattr(record, 'data'):
            log_record['data'] = record.data
        return json.dumps(log_record)

def setup_logger(name):
    """Sets up and returns a logger instance that outputs logs in JSON format."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = JsonFormatter('%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def log_info(logger, message, data=None):
    """Logs an informational message with optional additional data."""
    if data:
        logger.info(message, extra={'data': data})
    else:
        logger.info(message)

def log_error(logger, message, data=None):
    """Logs an error message with optional additional data."""
    if data:
        logger.error(message, extra={'data': data})
    else:
        logger.error(message)
    return False
