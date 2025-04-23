# E-Commerce Data Pipeline Project

This project implements an end-to-end data ingestion pipeline using Kafka, Python, and Snowflake for a small e-commerce store's real-time analytics.

## Project Structure

```
.
├── README.md
├── requirements.txt
├── .env
├── Dockerfile
├── docker-compose.yml
├── .github/
│   └── workflows/
│       └── ci-cd.yml
├── src/
│   ├── producer.py
│   ├── consumer.py
│   ├── config.py
│   ├── logger.py
│   ├── config_validator.py
│   └── health_check.py
└── sql/
    ├── schema.sql
    └── analytics.sql
```

## Setup Instructions

### 1. Prerequisites

- Python 3.8+
- Snowflake account
- Kafka cluster (local or cloud-based)
- Docker and Docker Compose (for containerized deployment)

### 2. Environment Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the root directory with your credentials:
```env
# Snowflake Credentials
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=E_COMMERCE
SNOWFLAKE_SCHEMA=E_COMMERCE

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=ecommerce-consumer
```

### 3. Snowflake Setup

1. Log in to your Snowflake account
2. Run the schema creation script:
```bash
snowsql -f sql/schema.sql
```

### 4. Running the Pipeline

#### Local Development

1. Start the Kafka producer:
```bash
python src/producer.py
```

2. Start the Kafka consumer:
```bash
python src/consumer.py
```

#### Docker Deployment

1. Build and start the services:
```bash
docker-compose up -d
```

2. Check service status:
```bash
docker-compose ps
```

3. View logs:
```bash
docker-compose logs -f
```

### 5. Health Checks

Run the health check to verify system components:
```bash
python src/health_check.py
```

### 6. Monitoring

- Logs are stored in the `logs` directory
- Health check status is available via Docker health check
- Metrics are exposed through logging

## Production Deployment

### 1. Environment Variables

For production deployment, set the following environment variables:
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier
- `SNOWFLAKE_USER`: Snowflake username
- `SNOWFLAKE_PASSWORD`: Snowflake password
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_GROUP_ID`: Kafka consumer group ID

### 2. Docker Deployment

1. Build the Docker image:
```bash
docker build -t ecommerce-pipeline .
```

2. Run the container:
```bash
docker run -d \
  --name ecommerce-pipeline \
  --env-file .env \
  ecommerce-pipeline
```

### 3. Kubernetes Deployment

1. Create a Kubernetes secret for credentials:
```bash
kubectl create secret generic ecommerce-secrets \
  --from-env-file=.env
```

2. Apply the Kubernetes manifests:
```bash
kubectl apply -f k8s/
```

## Project Components

1. **Kafka Producer**: Generates mock order data and sends it to the `orders` topic
2. **Kafka Consumer**: Consumes messages from the `orders` topic and loads them into Snowflake
3. **Snowflake Schema**: Implements a star schema for sales analytics
4. **Analytics Queries**: SQL queries for business intelligence

## Analytics Queries

The project includes several analytical queries in `sql/analytics.sql`:

1. **Monthly Sales Analysis**
   - Shows sales per product category per region
   - Helps identify regional trends and popular categories

2. **Customer Analysis**
   - Identifies top repeat customers
   - Tracks customers who order from multiple regions

3. **Product Performance**
   - Shows total quantity sold and average price per product
   - Helps with inventory and pricing decisions

4. **Slowly Changing Dimensions**
   - Tracks changes in customer information over time
   - Maintains historical data for analysis

## Notes

- The project uses a star schema design with fact and dimension tables
- Data is processed in real-time from Kafka to Snowflake
- Environment variables are used for secure credential management
- The schema includes proper indexing and constraints for efficient querying
- Docker and Kubernetes support for containerized deployment
- CI/CD pipeline with GitHub Actions
- Health checks and monitoring for production reliability 