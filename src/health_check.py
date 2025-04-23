import time
from typing import Dict, Any
import snowflake.connector
from confluent_kafka import Consumer
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SNOWFLAKE_CONFIG, KAFKA_CONFIG
from logger import setup_logger

logger = setup_logger("health_check", "health_check.log")


class HealthCheck:
    def __init__(self):
        self.snowflake_status = False
        self.kafka_status = False
        self.last_check_time = None

    def check_snowflake(self) -> bool:
        """Check Snowflake connection."""
        try:
            conn = snowflake.connector.connect(
                account=SNOWFLAKE_CONFIG["account"],
                user=SNOWFLAKE_CONFIG["user"],
                password=SNOWFLAKE_CONFIG["password"],
                database=SNOWFLAKE_CONFIG["database"],
                schema=SNOWFLAKE_CONFIG["schema"],
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            self.snowflake_status = True
            logger.info("Snowflake health check passed")
            return True
        except Exception as e:
            self.snowflake_status = False
            logger.error(f"Snowflake health check failed: {str(e)}")
            return False

    def check_kafka(self) -> bool:
        """Check Kafka connection."""
        try:
            consumer = Consumer(
                {
                    "bootstrap.servers": KAFKA_CONFIG["bootstrap.servers"],
                    "group.id": f"health-check-{int(time.time())}",
                    "auto.offset.reset": "earliest",
                }
            )
            consumer.subscribe(["orders"])
            consumer.poll(1.0)
            consumer.close()
            self.kafka_status = True
            logger.info("Kafka health check passed")
            return True
        except Exception as e:
            self.kafka_status = False
            logger.error(f"Kafka health check failed: {str(e)}")
            return False

    def run_health_check(self) -> Dict[str, Any]:
        """Run all health checks."""
        self.last_check_time = time.time()
        return {
            "timestamp": self.last_check_time,
            "snowflake": self.check_snowflake(),
            "kafka": self.check_kafka(),
            "overall_status": self.snowflake_status and self.kafka_status,
        }


def main():
    """Run health check and exit with appropriate status code."""
    health_check = HealthCheck()
    status = health_check.run_health_check()

    if status["overall_status"]:
        logger.info("All systems healthy")
        sys.exit(0)
    else:
        logger.error("Health check failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
