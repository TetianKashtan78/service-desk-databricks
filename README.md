# Service Desk Databricks Asset Bundle

A Databricks Asset Bundle project for building and orchestrating a medallion architecture data pipeline for service desk data.

## Overview

This project implements a three-layer medallion architecture (bronze, silver, gold) to transform raw service desk data into analytical-ready datasets using Databricks Workflows and Python scripts.

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

## Architecture

### Bronze Layer
Raw data ingestion from source systems. Minimal transformations, data is persisted as-is for audit trails.

### Silver Layer
Cleaned, deduplicated, and standardized data. Business rules are applied and data quality checks are enforced.

### Gold Layer
Aggregated, business-ready analytical datasets optimized for reporting and BI tools.

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

## Workflow Execution

The `service_desk_job.yml` defines a Databricks Workflow that orchestrates:

1. **Bronze Job** - `load_bronze.py` - Ingests raw service desk data
2. **Silver Job** - `build_silver.py` - Cleans and transforms data
3. **Gold Job** - `build_gold.py` - Creates analytical datasets

Each stage depends on successful completion of the previous stage.

## Monitoring & Debugging

Monitor job runs in the Databricks workspace:

```bash
# View job details
databricks jobs get --job-id <job_id>

# View run history
databricks runs list --job-id <job_id>

# View run logs
databricks runs get-output --run-id <run_id>
```

## Development

### Local Testing

Develop and test scripts locally before deployment:

```python
# Test bronze layer
python src/bronze/load_bronze.py

# Test silver layer
python src/silver/build_silver.py

# Test gold layer
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








