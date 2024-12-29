import os
from dotenv import load_dotenv

def load_config(env_path=".backupmate.env"):
    """Loads configuration parameters from the .env file."""
    # Only load if file exists, otherwise return empty config
    if not os.path.exists(env_path):
        return {key: None for key in [
            "DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD",
            "MARIADB_BACKUP_PATH", "S3_BUCKET_NAME",
            "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
            "AWS_REGION", "LOCAL_TEMP_DIR",
            "FULL_BACKUP_PREFIX", "INCREMENTAL_BACKUP_PREFIX",
            "FULL_BACKUP_SCHEDULE"
        ]}

    # Load with override to ensure we get values from the specified file
    load_dotenv(dotenv_path=env_path, override=True)
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
        if not config.get(param) or config[param].strip() == "":
            raise ValueError(f"Missing required configuration parameter: {param}")

    # Validate DB_PORT is an integer
    if config.get("DB_PORT"):
        try:
            int(config["DB_PORT"])
        except ValueError:
            raise ValueError("DB_PORT must be an integer")

    # Validate schedule format
    if config["FULL_BACKUP_SCHEDULE"] not in ["weekly", "monthly"]:
        raise ValueError("FULL_BACKUP_SCHEDULE must be either 'weekly' or 'monthly'")

    # Validate S3 prefix paths end with '/'
    if not config["FULL_BACKUP_PREFIX"].endswith("/"):
        raise ValueError("S3 prefix paths must end with '/'")
    if not config["INCREMENTAL_BACKUP_PREFIX"].endswith("/"):
        raise ValueError("S3 prefix paths must end with '/'")

    # Validate absolute paths
    if not os.path.isabs(config["MARIADB_BACKUP_PATH"]):
        raise ValueError("MARIADB_BACKUP_PATH must be an absolute path")
    if not os.path.isabs(config["LOCAL_TEMP_DIR"]):
        raise ValueError("LOCAL_TEMP_DIR must be an absolute path")

    return True
