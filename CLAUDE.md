# CLAUDE.md — bcn-etl-pipeline

End-to-end ETL pipeline ingesting Barcelona Open Data, transforming with dbt (medallion architecture), orchestrated with Apache Airflow, stored in PostgreSQL, fully containerized with Docker.

## Tech Stack

| Layer | Tool | Version |
|---|---|---|
| Language | Python | 3.12 |
| Transformation | dbt Core | latest |
| Warehouse | PostgreSQL | 16 |
| Orchestration | Apache Airflow | 2.x |
| Containers | Docker + Compose | latest |
| Testing | pytest + dbt tests | — |

## Project Structure

```
bcn-etl-pipeline/
├── dags/                        # Airflow DAG definitions
│   └── bcn_etl_dag.py
├── src/
│   ├── extract/                 # API ingestion (Barcelona Open Data)
│   │   ├── __init__.py
│   │   └── open_data_bcn.py
│   └── utils/
│       └── api_client.py
├── dbt_project/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/             # 1:1 with source, light cleaning
│   │   ├── intermediate/        # Business logic, joins
│   │   └── marts/               # Analytics-ready tables
│   ├── tests/                   # Custom dbt tests
│   └── macros/
├── tests/                       # Python/pytest tests
├── config/
│   └── sources.yml              # API source definitions
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── docs/
│   ├── architecture.md
│   └── screenshots/
├── .env.example                 # Environment variables template (never commit .env)
├── CLAUDE.md
└── README.md
```

## Essential Commands

### Docker / Infrastructure

```bash
# Start all services (PostgreSQL + Airflow + webserver)
docker-compose up -d

# Check running services
docker-compose ps

# View logs for a specific service
docker-compose logs -f airflow-webserver

# Stop all services
docker-compose down

# Full reset (destroys volumes — use with care)
docker-compose down -v
```

### dbt

```bash
cd dbt_project

# Install dependencies
dbt deps

# Run all models
dbt run

# Run specific layer
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Run tests
dbt test

# Generate and serve documentation
dbt docs generate
dbt docs serve

# Check for compilation errors without executing
dbt compile
```

### Python / Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Run all Python tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=term-missing

# Run a specific extraction script manually
python -m src.extract.open_data_bcn
```

### Airflow (once Docker is up)

```bash
# Access UI: http://localhost:8080  (user: airflow / pass: airflow)

# Trigger DAG manually from CLI
docker-compose exec airflow-webserver airflow dags trigger bcn_etl_dag

# List DAGs
docker-compose exec airflow-webserver airflow dags list
```

## Environment Variables

Copy `.env.example` to `.env` and fill in values. Never commit `.env`.

Key variables:
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- `AIRFLOW__CORE__FERNET_KEY`
- `BCN_API_BASE_URL` — Barcelona Open Data base URL
- `BCN_API_APP_ID`, `BCN_API_APP_CODE` — API credentials (if required)

## Data Architecture (Medallion)

```
Barcelona Open Data API
        │
        ▼
  staging/          ← raw ingestion, minimal transformations, 1:1 with source
        │
        ▼
  intermediate/     ← joins, business logic, calculations
        │
        ▼
  marts/            ← analytics-ready facts and dimensions
```

## Data Quality Rules

- All staging models: `not_null` + `unique` on primary keys
- All mart models: relationship tests to dimensions
- Custom dbt tests for range validation (e.g. air quality index bounds)
- Airflow SLA alerts on DAG runs > 30 min

## Development Guidelines

- **Never commit** `.env`, credentials, or `dbt_project/target/`
- dbt model names: `stg_`, `int_`, `fct_`, `dim_` prefixes strictly enforced
- Python modules in `src/` use absolute imports (`from src.extract...`)
- All API calls go through `src/utils/api_client.py` — do not use `requests` directly elsewhere
- Add a dbt test for every new model before marking it complete
- Docker Compose is the only supported local dev environment — do not run Airflow natively

## API: Barcelona Open Data

Base URL: `https://opendata-ajuntament.barcelona.cat/data/api/action/`

Key datasets used:
- Air quality measurements by station
- Weather / meteorological data
- Station metadata (location, type)

Authentication: some endpoints require `App-Id` + `App-Code` headers (register at opendata-ajuntament.barcelona.cat).
