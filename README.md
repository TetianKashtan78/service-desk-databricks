# Service Desk project

A Databricks Asset Bundle project for building and orchestrating a medallion architecture data pipeline for service desk data.

## Overview

This project implements a three-layer medallion architecture (bronze, silver, gold) to transform raw service desk data into analytical-ready datasets using Databricks Workflows and Python scripts.

## Tech Stack

- PySpark and SQL for data processing
- Databricks Runtime and Delta Lake
- Databricks Workflows for orchestration
- Databricks CLI for local validation and deployment
- GitHub Actions for CI/CD
- Version control with Git

## Architecture

The architecture follows a medallion design with separate layers for raw ingestion, cleaning, and analytics-ready output. Each layer performs specific transformations and validations to move data from raw source format to business-ready analytics tables.

### Bronze Layer
- Bronze: ingest raw service desk data from source feeds, logs, ticket exports, and event streams. Data is persisted in Delta Lake tables with schema enforcement and partitioning, while preserving original fields and metadata for auditability and lineage.

- Silver: apply data cleaning, normalization, deduplication, and standardization. This layer resolves inconsistent ticket statuses, maps user and department identifiers, filters invalid records, enriches with reference data, and applies business rules such as incident classification and priority assignment.

### Gold Layer
- Gold: build analytical datasets and KPI tables for reporting. This layer aggregates ticket counts, response and resolution time metrics, SLA compliance, and service-level performance. Gold tables are optimized for BI consumption and include summary views for dashboards and executive reporting.

## Data Model

The data model is built around service desk ticketing and operational metrics:

- Bronze layer stores raw ticket events, change logs, and user interactions.
- Silver layer stores cleaned ticket records, resolved incidents, escalations, and standardized fields.
- Gold layer stores summary tables with KPIs such as ticket volume, response times, resolution rates, and service-level performance.

## Project Structure

```
├── databricks.yml                    # Asset Bundle configuration
├── resources/
│   └── service_desk_job.yml         # Databricks Workflow definition
├── src/
│   ├── bronze/
│   │   └── load_bronze.py           # Raw data ingestion
│   ├── silver/
│   │   └── build_silver.py          # Data cleaning and transformation
│   └── gold/
│       └── build_gold.py            # Aggregated analytical layer
└── .github/
    └── workflows/
        └── deploy.yml               # CI/CD deployment pipeline
```


## Prerequisites

- Databricks workspace with admin access
- Databricks CLI configured locally
- Python 3.9+
- Git

## Installation & Deployment

### Local Setup

```bash
# Clone the repository
git clone <repository-url>
cd service-desk-databricks

# Install Databricks CLI
pip install databricks-cli
```

### Deploy with GitHub Actions

Push to main branch to trigger automated deployment via `.github/workflows/deploy.yml`:

```bash
git push origin main
```

### Manual Deployment

```bash
# Validate the asset bundle
databricks bundle validate

# Deploy to Databricks workspace
databricks bundle deploy

# Run the job
databricks bundle run service_desk_job
```

## Configuration

Edit `databricks.yml` to configure:
- Workspace URL and authentication
- Cluster configuration
- Catalog and schema names
- Job parameters

### GitHub Secrets

Configure the following GitHub Secrets for CI/CD deployment:
- `DATABRICKS_HOST` - Your Databricks workspace URL
- `DATABRICKS_TOKEN` - Your Databricks personal access token
- `AZURE_DATABASE_ACCESS_KEY` - Azure storage account key for authentication 

These secrets are used by the GitHub Actions workflow to authenticate and deploy to your Databricks workspace.

## Workflow Execution

The `service_desk_job.yml` defines a Databricks Workflow that orchestrates:

1. **Bronze Job** - `load_bronze.py` - Ingests raw service desk data
2. **Silver Job** - `build_silver.py` - Cleans and transforms data
3. **Gold Job** - `build_gold.py` - Creates analytical datasets

Each stage depends on successful completion of the previous stage.

## Development

### Local Testing

These scripts are Python entry points that use PySpark and SQL in a Databricks context. You can run them locally for syntax validation and unit testing, but a full execution requires a configured Spark environment.

```bash
# Validate bronze layer script
python src/bronze/load_bronze.py

# Validate silver layer script
python src/silver/build_silver.py

# Validate gold layer script
python src/gold/build_gold.py
```

### Code Quality

Ensure code follows best practices:
- Use type hints
- Add docstrings to functions
- Follow PEP 8 style guidelines

## CI/CD Pipeline

The GitHub Actions workflow in `.github/workflows/deploy.yml` automatically:
- Validates the bundle configuration
- Deploys changes to Databricks
- Runs integration tests
- Triggers the job pipeline








