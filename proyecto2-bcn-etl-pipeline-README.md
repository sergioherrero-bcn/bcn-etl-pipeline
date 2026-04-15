# bcn-etl-pipeline — End-to-End Data Pipeline with Barcelona Open Data

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)

> Production-grade ETL pipeline that ingests Barcelona open data, transforms it with dbt, and serves analytics-ready tables — fully orchestrated with Airflow and containerized with Docker.

<!-- ![Pipeline Architecture](docs/screenshots/airflow-dag.png) -->

## Overview

This project demonstrates a complete, production-style data pipeline built with modern data engineering tools. It ingests data from Barcelona's Open Data API, applies layered transformations using dbt's medallion architecture (staging → intermediate → marts), orchestrates everything with Apache Airflow, and stores the results in PostgreSQL for downstream analytics.

### Why this project?

Building ETL scripts is easy. Building a **maintainable, tested, documented, and orchestrated** pipeline is what separates junior from senior data engineers. This project showcases the full workflow a production team would use.

## Architecture

```
┌─────────────────┐
│ Barcelona Open   │
│ Data API         │
└────────┬────────┘
         │ HTTP/JSON
         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  EXTRACT         │────▶│  TRANSFORM       │────▶│  LOAD            │
│  Python scripts  │     │  dbt models      │     │  PostgreSQL      │
│  (API ingestion) │     │  (SQL + Jinja)   │     │  (analytics-     │
│                  │     │                  │     │   ready tables)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                       │                        │
         └───────────┬───────────┘                        │
                     ▼                                    │
              ┌──────────────┐                            │
              │  Apache       │◀───────────────────────────┘
              │  Airflow      │  (orchestration + monitoring)
              └──────────────┘
                     │
              ┌──────┴──────┐
              │   Docker     │
              │   Compose    │
              └─────────────┘
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Ingestion | Python + requests | Extract data from REST APIs |
| Transformation | dbt Core | SQL-based transformations, testing, documentation |
| Storage | PostgreSQL 16 | Data warehouse |
| Orchestration | Apache Airflow 2.x | DAG scheduling, monitoring, alerting |
| Containerization | Docker + Docker Compose | Reproducible infrastructure |
| Testing | dbt tests + pytest | Data quality + code quality |

## Quick Start

```bash
# Clone
git clone https://github.com/sergioherrero/bcn-etl-pipeline.git
cd bcn-etl-pipeline

# Start all services
docker-compose up -d

# Access Airflow UI
open http://localhost:8080    # user: airflow / pass: airflow

# Trigger the DAG manually or wait for scheduled run
```

## dbt Models

The transformation layer follows the **medallion architecture**:

```
models/
├── staging/           # 1:1 with source tables, light cleaning
│   ├── stg_air_quality.sql
│   ├── stg_stations.sql
│   └── stg_measurements.sql
├── intermediate/      # Business logic, joins, calculations
│   ├── int_daily_averages.sql
│   └── int_station_metrics.sql
└── marts/             # Analytics-ready tables
    ├── fct_air_quality_daily.sql
    └── dim_stations.sql
```

## Data Quality

- **dbt tests**: not_null, unique, accepted_values, relationships
- **Custom tests**: range validation, freshness checks
- **Airflow**: DAG failure alerts, SLA monitoring

## Project Structure

```
bcn-etl-pipeline/
├── dags/                    # Airflow DAG definitions
│   └── bcn_etl_dag.py
├── src/
│   ├── extract/             # API ingestion scripts
│   │   ├── __init__.py
│   │   └── open_data_bcn.py
│   └── utils/
│       └── api_client.py
├── dbt_project/
│   ├── dbt_project.yml
│   ├── models/
│   ├── tests/
│   └── macros/
├── tests/                   # Python tests
├── config/
│   └── sources.yml
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── docs/
│   ├── architecture.md
│   └── screenshots/
├── LICENSE
└── README.md
```

## What I Learned

- Orchestrating multi-step data pipelines with Airflow (DAG design, task dependencies, error handling)
- Modeling data transformations with dbt (staging/intermediate/marts, testing, documentation)
- Designing a containerized data stack that runs with a single `docker-compose up`
- Working with real-world API data: handling rate limits, schema changes, and missing data

## Next Steps

- [ ] Add Streamlit dashboard for data exploration
- [ ] Implement incremental loads in dbt
- [ ] Add Great Expectations for advanced data validation
- [ ] Deploy to AWS (S3 + RDS + MWAA)

## Author

**Sergio Herrero** — [LinkedIn](https://linkedin.com/in/sergioherrero) · [GitHub](https://github.com/sergioherrero)

## License

MIT
