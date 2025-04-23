import os
import pandas as pd
from sqlalchemy import create_engine
import dotenv

dotenv.load_dotenv()


class SnowflakeAnalytics:
    def __init__(self):
        connection_string = f"snowflake://{os.environ.get('SNOWFLAKE_USER')}:{os.environ.get('SNOWFLAKE_PASSWORD')}@{os.environ.get('SNOWFLAKE_ACCOUNT')}/{os.environ.get('SNOWFLAKE_DATABASE')}/{os.environ.get('SNOWFLAKE_SCHEMA')}?warehouse={os.environ.get('SNOWFLAKE_WAREHOUSE')}"
        self.engine = create_engine(connection_string)

    def get_monthly_sales(self, year: int = None, month: int = None) -> pd.DataFrame:
        """Get monthly sales per product category per region."""
        query = """
        SELECT 
            d.year,
            d.month,
            p.category,
            c.customer_region,
            COUNT(DISTINCT f.order_id) as total_orders,
            SUM(f.quantity) as total_quantity,
            SUM(f.total_amount) as total_sales,
            AVG(f.total_amount) as average_order_value
        FROM FACT_ORDERS f
        JOIN DIM_DATE d ON f.date_id = d.date_id
        JOIN DIM_PRODUCT p ON f.product_id = p.product_id
        JOIN DIM_CUSTOMER c ON f.customer_id = c.customer_id
        """
        if year and month:
            query += f" WHERE d.year = {year} AND d.month = {month}"
        else:
            query += " WHERE d.year = EXTRACT(YEAR FROM CURRENT_DATE()) AND d.month = EXTRACT(MONTH FROM CURRENT_DATE())"

        query += """ 
        GROUP BY d.year, d.month, p.category, c.customer_region
        ORDER BY d.year, d.month, p.category, c.customer_region"""

        return pd.read_sql(query, self.engine)

    def get_top_repeat_customers(self, days: int = 30) -> pd.DataFrame:
        """Get top 5 repeat customers in the specified number of days."""
        query = """
        WITH customer_orders AS (
            SELECT 
                c.customer_id,
                c.customer_name,
                c.customer_region,
                COUNT(DISTINCT f.order_id) as order_count,
                SUM(f.total_amount) as total_spent,
                AVG(f.total_amount) as average_order_value
            FROM FACT_ORDERS f
            JOIN DIM_CUSTOMER c ON f.customer_id = c.customer_id
            WHERE f.order_timestamp >= DATEADD(day, %(days)s, CURRENT_TIMESTAMP())
            GROUP BY c.customer_id, c.customer_name, c.customer_region
        )
        SELECT 
            customer_id,
            customer_name,
            customer_region,
            order_count,
            total_spent,
            average_order_value
        FROM customer_orders
        ORDER BY order_count DESC, total_spent DESC
        LIMIT 5
        """
        return pd.read_sql(query, self.engine, params={"days": -days})

    def get_product_sales_stats(self) -> pd.DataFrame:
        """Get total quantity sold and average price per product."""
        query = """
        SELECT 
            p.product_id,
            p.product_name,
            p.category,
            SUM(f.quantity) as total_quantity_sold,
            AVG(f.total_amount / f.quantity) as average_price
        FROM FACT_ORDERS f
        JOIN DIM_PRODUCT p ON f.product_id = p.product_id
        GROUP BY p.product_id, p.product_name, p.category
        ORDER BY total_quantity_sold DESC
        """
        return pd.read_sql(query, self.engine)

    def get_multi_region_customers(self) -> pd.DataFrame:
        """Get customers who placed orders in multiple regions."""
        query = """
        WITH customer_regions AS (
            SELECT 
                c.customer_id,
                c.customer_name,
                COUNT(DISTINCT c.customer_region) as region_count
            FROM FACT_ORDERS f
            JOIN DIM_CUSTOMER c ON f.customer_id = c.customer_id
            GROUP BY c.customer_id, c.customer_name
        )
        SELECT 
            customer_id,
            customer_name,
            region_count
        FROM customer_regions
        WHERE region_count > 1
        ORDER BY region_count DESC;
        """
        return pd.read_sql(query, self.engine)

    def close(self):
        """Close the Snowflake connection."""
        if hasattr(self, "engine"):
            self.engine.dispose()


def main():
    """Example usage of the SnowflakeAnalytics class."""
    try:
        analytics = SnowflakeAnalytics()

        print("Monthly Sales Analysis:")
        monthly_sales = analytics.get_monthly_sales(2025, 4)
        print(monthly_sales.head())

        print("\nTop Repeat Customers:")
        top_customers = analytics.get_top_repeat_customers()
        print(top_customers)

        print("\nProduct Sales Statistics:")
        product_stats = analytics.get_product_sales_stats()
        print(product_stats.head())

        print("\nMulti-region Customers:")
        multi_region = analytics.get_multi_region_customers()
        if not multi_region.empty:
            print(multi_region)
        else:
            print("No customers found who placed orders in multiple regions.")

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        analytics.close()


if __name__ == "__main__":
    main()
