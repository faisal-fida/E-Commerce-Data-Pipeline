from typing import Dict, Any
import sys


class ConfigError(Exception):
    """Custom exception for configuration errors."""

    pass


def validate_snowflake_config(config: Dict[str, Any]) -> None:
    """Validate Snowflake configuration."""
    required_fields = ["account", "user", "password", "database", "schema"]
    missing_fields = [field for field in required_fields if not config.get(field)]

    if missing_fields:
        raise ConfigError(
            f"Missing required Snowflake configuration fields: {', '.join(missing_fields)}"
        )

    # Validate account format
    if not config["account"].endswith(".snowflakecomputing.com"):
        config["account"] = f"{config['account']}.snowflakecomputing.com"


def validate_kafka_config(config: Dict[str, Any]) -> None:
    """Validate Kafka configuration."""
    required_fields = ["bootstrap.servers", "group.id"]
    missing_fields = [field for field in required_fields if not config.get(field)]

    if missing_fields:
        raise ConfigError(
            f"Missing required Kafka configuration fields: {', '.join(missing_fields)}"
        )


def validate_config() -> None:
    """Validate all configuration settings."""
    try:
        from config import SNOWFLAKE_CONFIG, KAFKA_CONFIG

        validate_snowflake_config(SNOWFLAKE_CONFIG)
        validate_kafka_config(KAFKA_CONFIG)

    except ImportError as e:
        raise ConfigError(f"Failed to import configuration: {str(e)}")
    except Exception as e:
        raise ConfigError(f"Configuration validation failed: {str(e)}")


if __name__ == "__main__":
    try:
        validate_config()
        print("Configuration validation successful!")
    except ConfigError as e:
        print(f"Configuration validation failed: {str(e)}")
        sys.exit(1)
