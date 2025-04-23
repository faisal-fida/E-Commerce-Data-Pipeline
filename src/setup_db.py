import snowflake.connector
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SNOWFLAKE_CONFIG


def setup_tables():
    """Create the required tables in Snowflake."""
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG["account"],
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
    )

    cursor = conn.cursor()

    try:
        with open("sql/setup_tables.sql", "r") as file:
            sql_commands = file.read()

        # Split and execute multiple SQL commands
        for command in sql_commands.split(";"):
            if command.strip():
                cursor.execute(command)

        print("Tables created successfully!")

    except Exception as e:
        print(f"Error creating tables: {str(e)}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    setup_tables()
