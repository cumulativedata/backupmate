import os
from dotenv import load_dotenv

def load_config(env_path=".backupmate.env"):
    """Loads configuration parameters from the .env file."""
    load_dotenv(dotenv_path=env_path)
    config = {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_PORT": os.getenv("DB_PORT"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "MARIADB_BACKUP_PATH": os.getenv("MARIADB_BACKUP_PATH"),
        "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_REGION"),
        "LOCAL_TEMP_DIR": os.getenv("LOCAL_TEMP_DIR"),
        "FULL_BACKUP_PREFIX": os.getenv("FULL_BACKUP_PREFIX"),
        "INCREMENTAL_BACKUP_PREFIX": os.getenv("INCREMENTAL_BACKUP_PREFIX"),
        "FULL_BACKUP_SCHEDULE": os.getenv("FULL_BACKUP_SCHEDULE"),
    }
    return config

def validate_config(config):
    """Validates the loaded configuration to ensure all required parameters are present."""
    required_params = [
        "DB_HOST",
        "DB_PORT",
        "DB_USER",
        "DB_PASSWORD",
        "MARIADB_BACKUP_PATH",
        "S3_BUCKET_NAME",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_REGION",
        "LOCAL_TEMP_DIR",
        "FULL_BACKUP_PREFIX",
        "INCREMENTAL_BACKUP_PREFIX",
        "FULL_BACKUP_SCHEDULE",
    ]
    for param in required_params:
        if not config.get(param):
            raise ValueError(f"Missing required configuration parameter: {param}")
    if config.get("DB_PORT"):
        try:
            int(config["DB_PORT"])
        except ValueError:
            raise ValueError("DB_PORT must be an integer")
    return True
