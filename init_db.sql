-- Initialise raw schemas for the BCN ETL pipeline.
-- Runs once when the PostgreSQL container is first created.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- Raw tables are created by the Python extractors on first run.
-- This file only ensures the schemas exist so dbt can reference them.
