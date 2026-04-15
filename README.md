# Real-Time Customer Activity Analytics Platform

## Project Overview

This project demonstrates how to build a real-time data analytics platform that processes customer activity events such as page views, add-to-cart actions, checkouts, and purchases. The system ingests streaming data, processes it in near real time, and serves analytical insights through interactive dashboards.

The goal of this project is to simulate a production-grade data pipeline used in e-commerce and product analytics systems, where timely insights are critical for decision-making.

---

## Problem Statement

Modern applications generate a continuous stream of user interaction data. Businesses need to:

- Track user activity in real time  
- Analyze customer behavior  
- Monitor product performance  
- Identify drop-offs in the purchase funnel  

Traditional batch processing systems are not sufficient for these use cases. This project addresses the need for low-latency, scalable data processing by implementing a streaming architecture.

---

## Architecture Overview

The system follows a streaming architecture:

1. A Python-based producer simulates user activity and sends events to Kafka  
2. Kafka acts as a distributed event streaming platform to buffer and manage incoming data  
3. Spark Structured Streaming consumes events from Kafka and processes them incrementally  
4. The pipeline performs data cleaning, schema enforcement, deduplication, and time-based aggregations  
5. Processed data is stored in ClickHouse for fast analytical queries  
6. Dashboards are created in Metabase to visualize key metrics  

---

## Tech Stack

- Python  
- Apache Kafka  
- Apache Spark Structured Streaming  
- ClickHouse  
- Metabase  
- Docker Compose  

---

## Key Features

- Real-time ingestion of customer activity events  
- Event-time processing with watermarking and late data handling  
- Deduplication based on unique event identifiers  
- Windowed aggregations for near real-time analytics  
- Storage of both raw and aggregated data  
- Interactive dashboards for business insights  

---

## Data Pipeline Flow

1. Synthetic events are generated and sent to Kafka  
2. Kafka streams the data to Spark Structured Streaming  
3. Spark parses JSON data and enforces schema consistency  
4. Duplicate events are removed using unique identifiers  
5. Time-based aggregations are computed:
   - Active users per 5 minutes  
   - Event counts per type  
   - Revenue and purchases per product per hour  
6. Processed data is stored in ClickHouse  
7. Metabase queries ClickHouse to display dashboards  

---

## Data Model

The project is designed with two logical layers:

### Raw Data Layer
Stores all incoming events with minimal transformation. This ensures data traceability and supports reprocessing.

### Aggregated Layer
Stores processed metrics used for analytics and reporting, including event counts, active users, and revenue metrics.

---

## Dashboard Insights

The dashboard provides insights such as:

- Event distribution by type  
- Events over time  
- Revenue by product  
- Active users by event type  
- Trend analysis for user activity and revenue  

These metrics help in understanding customer behavior and system performance.

---

## Learning Outcomes

This project demonstrates practical implementation of:

- Real-time data pipelines  
- Event-driven architecture  
- Kafka and stream processing fundamentals  
- Spark Structured Streaming concepts such as windowing and watermarking  
- OLAP data modeling with ClickHouse  
- Dashboard creation for business analytics  

---

## Future Improvements

- Integrate Airflow for orchestration and scheduling  
- Add data quality validation checks  
- Implement monitoring and alerting  
- Extend pipeline with object storage (S3/MinIO)  
- Add funnel conversion and session-based analytics  
- Deploy the system on cloud infrastructure  

