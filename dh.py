import plotly.graph_objects as go
from plotly.subplots import make_subplots
from analytics import SnowflakeAnalytics
import datetime


def create_dashboard():
    # Initialize analytics
    analytics = SnowflakeAnalytics()

    # Create a subplot figure
    fig = make_subplots(
        rows=3,
        cols=2,
        subplot_titles=(
            "Monthly Sales by Region",
            "Top 5 Repeat Customers",
            "Product Category Distribution",
            "Average Price by Category",
            "Multi-region Customer Analysis",
            "Sales Trend",
        ),
        specs=[
            [{"type": "bar"}, {"type": "bar"}],
            [{"type": "pie"}, {"type": "bar"}],
            [{"type": "bar"}, {"type": "scatter"}],
        ],
    )

    try:
        # 1. Monthly Sales Analysis
        monthly_sales = analytics.get_monthly_sales(2025, 4)
        sales_by_region = monthly_sales.groupby("customer_region")["total_sales"].sum()

        fig.add_trace(
            go.Bar(x=sales_by_region.index, y=sales_by_region.values, name="Sales by Region"),
            row=1,
            col=1,
        )

        # 2. Top Customers Analysis
        top_customers = analytics.get_top_repeat_customers()
        fig.add_trace(
            go.Bar(
                x=top_customers["customer_name"], y=top_customers["order_count"], name="Order Count"
            ),
            row=1,
            col=2,
        )

        # 3. Product Category Distribution
        product_stats = analytics.get_product_sales_stats()
        category_sales = product_stats.groupby("category")["total_quantity_sold"].sum()

        fig.add_trace(
            go.Pie(
                labels=category_sales.index,
                values=category_sales.values,
                name="Category Distribution",
            ),
            row=2,
            col=1,
        )

        # 4. Average Price by Category
        avg_price_by_category = product_stats.groupby("category")["average_price"].mean()
        fig.add_trace(
            go.Bar(x=avg_price_by_category.index, y=avg_price_by_category.values, name="Avg Price"),
            row=2,
            col=2,
        )

        # 5. Multi-region Customer Analysis
        multi_region = analytics.get_multi_region_customers()
        fig.add_trace(
            go.Bar(
                x=multi_region["customer_name"],
                y=multi_region["region_count"],
                name="Regions per Customer",
            ),
            row=3,
            col=1,
        )

        # 6. Sales Trend (using monthly data)
        sales_trend = monthly_sales.groupby("month")["total_sales"].sum()
        fig.add_trace(
            go.Scatter(
                x=sales_trend.index, y=sales_trend.values, mode="lines+markers", name="Sales Trend"
            ),
            row=3,
            col=2,
        )

        # Update layout
        fig.update_layout(
            title_text="Sales Analytics Dashboard",
            showlegend=True,
            height=1200,
            width=1600,
            template="plotly_white",
        )

        # Export to PDF
        today = datetime.datetime.now().strftime("%Y%m%d")
        pdf_filename = f"sales_dashboard_{today}.pdf"
        fig.write_image(pdf_filename)

        # Also save as HTML for interactive viewing
        html_filename = f"sales_dashboard_{today}.html"
        fig.write_html(html_filename)

        print(f"Dashboard exported successfully to {pdf_filename} and {html_filename}")

    except Exception as e:
        print(f"Error creating dashboard: {str(e)}")
    finally:
        analytics.close()


if __name__ == "__main__":
    create_dashboard()
