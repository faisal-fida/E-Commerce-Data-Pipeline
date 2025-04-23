import plotly.graph_objects as go
from plotly.subplots import make_subplots
from analytics import SnowflakeAnalytics
import datetime


def create_dashboard():
    # Initialize analytics
    analytics = SnowflakeAnalytics()

    # Create a subplot figure
    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Monthly Sales by Category and Region",
            "Top 5 Repeat Customers (Last 30 Days)",
            "Product Sales Statistics",
            "Multi-region Customer Analysis",
        ),
        specs=[
            [{"type": "bar"}, {"type": "bar"}],
            [{"type": "bar"}, {"type": "bar"}],
        ],
    )

    try:
        # 1. Monthly Sales Analysis
        monthly_sales = analytics.get_monthly_sales()
        sales_by_category_region = monthly_sales.pivot_table(
            values="total_sales", index="category", columns="customer_region", aggfunc="sum"
        ).fillna(0)

        for region in sales_by_category_region.columns:
            fig.add_trace(
                go.Bar(
                    x=sales_by_category_region.index,
                    y=sales_by_category_region[region],
                    name=region,
                ),
                row=1,
                col=1,
            )

        # 2. Top Customers Analysis
        top_customers = analytics.get_top_repeat_customers()
        fig.add_trace(
            go.Bar(
                x=top_customers["customer_name"],
                y=top_customers["order_count"],
                text=top_customers["total_spent"].round(2),
                textposition="auto",
                name="Order Count",
            ),
            row=1,
            col=2,
        )

        # 3. Product Sales Statistics
        product_stats = analytics.get_product_sales_stats()
        fig.add_trace(
            go.Bar(
                x=product_stats["product_name"],
                y=product_stats["total_quantity_sold"],
                text=product_stats["average_price"].round(2),
                textposition="auto",
                name="Quantity Sold",
            ),
            row=2,
            col=1,
        )

        # 4. Multi-region Customer Analysis
        multi_region = analytics.get_multi_region_customers()
        if not multi_region.empty:
            fig.add_trace(
                go.Bar(
                    x=multi_region["customer_name"],
                    y=multi_region["region_count"],
                    text=multi_region["region_count"],
                    textposition="auto",
                    name="Regions per Customer",
                ),
                row=2,
                col=2,
            )

        # Update layout
        fig.update_layout(
            title_text="Sales Analytics Dashboard",
            showlegend=True,
            height=1000,
            width=1400,
            template="plotly_white",
            barmode="group",
        )

        # Update axes labels
        fig.update_xaxes(title_text="Category", row=1, col=1)
        fig.update_xaxes(title_text="Customer", row=1, col=2)
        fig.update_xaxes(title_text="Product", row=2, col=1)
        fig.update_xaxes(title_text="Customer", row=2, col=2)

        fig.update_yaxes(title_text="Total Sales", row=1, col=1)
        fig.update_yaxes(title_text="Order Count", row=1, col=2)
        fig.update_yaxes(title_text="Quantity Sold", row=2, col=1)
        fig.update_yaxes(title_text="Number of Regions", row=2, col=2)

        # Export to PDF and HTML
        today = datetime.datetime.now().strftime("%Y%m%d")
        pdf_filename = f"sales_dashboard_{today}.pdf"
        html_filename = f"sales_dashboard_{today}.html"

        fig.write_image(pdf_filename)
        fig.write_html(html_filename)

        print(f"Dashboard exported successfully to {pdf_filename} and {html_filename}")

    except Exception as e:
        print(f"Error creating dashboard: {str(e)}")
    finally:
        analytics.close()


if __name__ == "__main__":
    create_dashboard()
