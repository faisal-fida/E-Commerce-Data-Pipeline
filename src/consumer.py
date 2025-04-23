import json
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import snowflake.connector
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_CONFIG, SNOWFLAKE_CONFIG


def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    return snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG["account"],
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
    )


def insert_dimension_data(conn, table_name, data):
    """Insert or update dimension table data."""
    cursor = conn.cursor()

    if table_name == "DIM_CUSTOMER":
        query = """
        INSERT INTO DIM_CUSTOMER (customer_id, customer_name, customer_region)
        VALUES (%s, %s, %s)
        ON CONFLICT (customer_id) DO UPDATE
        SET customer_name = EXCLUDED.customer_name,
            customer_region = EXCLUDED.customer_region,
            updated_at = CURRENT_TIMESTAMP()
        """
        values = (data["customer_id"], data["customer_name"], data["customer_region"])

    elif table_name == "DIM_PRODUCT":
        query = """
        INSERT INTO DIM_PRODUCT (product_id, product_name, category, price)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (product_id) DO UPDATE
        SET product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            price = EXCLUDED.price,
            updated_at = CURRENT_TIMESTAMP()
        """
        values = (data["product_id"], data["product_name"], data["category"], data["price"])

    cursor.execute(query, values)
    conn.commit()
    cursor.close()


def insert_fact_data(conn, order_data):
    """Insert fact table data."""
    cursor = conn.cursor()

    # Extract date from timestamp
    order_date = datetime.fromisoformat(order_data["order_timestamp"].replace("Z", "+00:00")).date()

    query = """
    INSERT INTO FACT_ORDERS (
        order_id, order_timestamp, customer_id, product_id,
        date_id, quantity, total_amount
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    values = (
        order_data["order_id"],
        order_data["order_timestamp"],
        order_data["customer_id"],
        order_data["product_id"],
        order_date,
        order_data["quantity"],
        order_data["price"] * order_data["quantity"],
    )

    cursor.execute(query, values)
    conn.commit()
    cursor.close()


def process_message(msg, conn):
    """Process a Kafka message and load it into Snowflake."""
    try:
        # Parse message
        order_data = json.loads(msg.value().decode("utf-8"))

        # Insert dimension data
        customer_data = {
            "customer_id": order_data["customer_id"],
            "customer_name": order_data["customer_name"],
            "customer_region": order_data["customer_region"],
        }
        insert_dimension_data(conn, "DIM_CUSTOMER", customer_data)

        product_data = {
            "product_id": order_data["product_id"],
            "product_name": order_data["product_name"],
            "category": order_data["category"],
            "price": order_data["price"],
        }
        insert_dimension_data(conn, "DIM_PRODUCT", product_data)

        # Insert fact data
        insert_fact_data(conn, order_data)

        print(f"Successfully processed order {order_data['order_id']}")

    except Exception as e:
        print(f"Error processing message: {str(e)}")


def main():
    # Configure consumer
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_CONFIG["bootstrap.servers"],
            "group.id": KAFKA_CONFIG["group.id"],
            "auto.offset.reset": "earliest",
        }
    )

    # Subscribe to topic
    consumer.subscribe(["orders"])

    # Get Snowflake connection
    conn = get_snowflake_connection()

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            process_message(msg, conn)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
