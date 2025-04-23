import os
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "E_COMMERCE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "E_COMMERCE"),
}

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": os.getenv("KAFKA_GROUP_ID", "ecommerce-consumer"),
}
