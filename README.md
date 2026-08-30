# SentinelPay Streaming Lakehouse

Production-grade end-to-end streaming lakehouse for payment data using Kafka, PySpark, Spark Structured Streaming, Delta Lake, Apache Airflow, dbt, Great Expectations, and MinIO.

## Overview
SentinelPay is a fintech-style data engineering project that simulates real-time payment and support-event processing on a modern lakehouse stack. The platform ingests event data, processes it through Bronze, Silver, and Gold layers, validates quality, orchestrates workflows with Airflow, and publishes analytics-ready datasets with dbt lineage and documentation.

## Architecture
- Ingestion: Kafka event streams and synthetic/reference inputs
- Processing: PySpark and Spark Structured Streaming
- Storage: Delta Lake on MinIO
- Orchestration: Apache Airflow
- Transformation: dbt
- Data Quality: Great Expectations
- Metadata and Lineage: dbt docs and lineage graph

## Key Features
- Real-time streaming ingestion for payment-related events
- Bronze, Silver, and Gold medallion architecture
- Checkpoint-based recovery and restart support
- Deduplication and idempotent processing
- Late-event and bad-record handling
- Quarantine flow for invalid records
- Airflow DAG orchestration across pipeline stages
- dbt models, documentation, and lineage
- Scale-tested pipeline execution on high-volume data

## Tech Stack
- Apache Kafka
- PySpark
- Spark Structured Streaming
- Delta Lake
- Apache Airflow
- dbt
- Great Expectations
- MinIO
- Python
- SQL
- Docker Compose

## Main Pipelines
- `sentinelpay_streaming_control`
- `sentinelpay_quality_gate`
- `sentinelpay_gold_pipeline`
- `sentinelpay_dbt_pipeline`
- `sentinelpay_main_pipeline`

## Project Structure
```text
airflow/         Airflow DAGs and orchestration logic
architecture/    Architecture and governance documentation
configs/         Configuration files
data_generator/  Synthetic data generation utilities
datasets/        Reference CSV datasets
dbt/             dbt project, models, sources, docs
diagrams/        Architecture diagrams
docker/          Container-related setup
docs/            Supporting documentation
screenshots/     Execution proof screenshots
src/             Streaming, batch, quality, and transformation code
tests/           Test cases and validation helpers



Data Quality and Governance:

This project includes a practical governance foundation through Great Expectations validations, Airflow quality-gate workflows, quarantine handling, layered Bronze-Silver-Gold design, and metadata/lineage visibility through dbt docs.


Running the Project:
docker compose up -d

Main local UIs:
- Airflow: http://localhost:8080
- dbt Docs: http://localhost:8081
- MinIO: http://localhost:9001
- Spark UI: http://localhost:4040


Proof of Execution
The project includes proof through Airflow DAG success runs, Spark UI screenshots, dbt lineage, MinIO Bronze/Silver/Gold folders, checkpoint and quarantine paths, and scale-validation outputs.


Future Improvements
- Schema registry integration
- Role-based access control
- Monitoring and alerting dashboards
- Data contracts for producers and consumers
- CI/CD automation for deployment and testing


## Architecture
![SentinelPay Architecture](architecture/sentinelpay-architecture.png)

## Execution Proof

### Airflow Orchestration
![Quality Gate Success](screenshots/airflow/03-quality-gate-success.png)
![Gold Pipeline Success](screenshots/airflow/04-gold-pipeline-success.png)

### Spark Streaming
![Streaming Query](screenshots/spark/02-streaming-query.png)
![Spark Stages](screenshots/spark/03-stages.png)

### dbt Lineage
![dbt Lineage](screenshots/dbt/01-lineage-graph.png)

### MinIO Lakehouse Storage
![Lakehouse Root](screenshots/minio/01-lakehouse-root.png)
![Silver Layer](screenshots/minio/02-silver-layer.png)
![Gold Layer](screenshots/minio/03-gold-layer.png)
![Checkpoint Folder](screenshots/minio/04-checkpoint-folder.png)