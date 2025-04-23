-- Create database and schema
CREATE DATABASE IF NOT EXISTS E_COMMERCE;
USE DATABASE E_COMMERCE;
CREATE SCHEMA IF NOT EXISTS E_COMMERCE;
USE SCHEMA E_COMMERCE;

-- Dimension Tables

-- Customer Dimension
CREATE TABLE IF NOT EXISTS DIM_CUSTOMER (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    customer_region VARCHAR(50),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Product Dimension
CREATE TABLE IF NOT EXISTS DIM_PRODUCT (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMBER(10,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Date Dimension
CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_id DATE PRIMARY KEY,
    year NUMBER(4),
    month NUMBER(2),
    day NUMBER(2),
    quarter NUMBER(1),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Fact Table
CREATE TABLE IF NOT EXISTS FACT_ORDERS (
    order_id VARCHAR(50) PRIMARY KEY,
    order_timestamp TIMESTAMP_NTZ,
    customer_id VARCHAR(50) REFERENCES DIM_CUSTOMER(customer_id),
    product_id VARCHAR(50) REFERENCES DIM_PRODUCT(product_id),
    date_id DATE REFERENCES DIM_DATE(date_id),
    quantity NUMBER(10),
    total_amount NUMBER(10,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create a stream for real-time data ingestion
CREATE STREAM IF NOT EXISTS ORDERS_STREAM ON TABLE FACT_ORDERS;

-- Create a task to populate the date dimension
create or replace task E_COMMERCE.E_COMMERCE.POPULATE_DATE_DIM
    warehouse=COMPUTE_WH
    schedule='1 MINUTE'
    as INSERT INTO DIM_DATE
    SELECT 
        DATEADD(day, SEQ4(), CURRENT_DATE()) as date_value,
        YEAR(date_value),
        MONTH(date_value),
        DAY(date_value),
        QUARTER(date_value),
        DAYOFWEEK(date_value) IN (1, 7),
        FALSE -- You can add holiday logic here
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
    WHERE date_value NOT IN (SELECT date_id FROM DIM_DATE);