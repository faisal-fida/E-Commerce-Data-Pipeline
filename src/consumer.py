import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import snowflake.connector
import sys
import os

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
        MERGE INTO DIM_CUSTOMER target
        USING (SELECT %s as customer_id, %s as customer_name, %s as customer_region) source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET 
                customer_name = source.customer_name,
                customer_region = source.customer_region,
                updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (customer_id, customer_name, customer_region)
            VALUES (source.customer_id, source.customer_name, source.customer_region)
        """
        values = (data["customer_id"], data["customer_name"], data["customer_region"])

    elif table_name == "DIM_PRODUCT":
        query = """
        MERGE INTO DIM_PRODUCT target
        USING (SELECT %s as product_id, %s as product_name, %s as category, %s as price) source
        ON target.product_id = source.product_id
        WHEN MATCHED THEN
            UPDATE SET 
                product_name = source.product_name,
                category = source.category,
                price = source.price,
                updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (product_id, product_name, category, price)
            VALUES (source.product_id, source.product_name, source.category, source.price)
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
        order_data = json.loads(msg.value().decode("utf-8"))

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

        insert_fact_data(conn, order_data)

        print(f"Successfully processed order {order_data['order_id']}")

    except Exception as e:
        print(f"Error processing message: {str(e)}")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_CONFIG["bootstrap.servers"],
            "group.id": KAFKA_CONFIG["group.id"],
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe(["orders"])

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
