/*
    Snowflake Initialization Script
    Sets up the optimized warehouse, database, schema, final target table, and external stage.
*/

-- Create a dedicated warehouse for migration with cost-optimizations (AUTO_SUSPEND)
CREATE OR REPLACE WAREHOUSE MIGRATION_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Optimized warehouse for legacy trade data migration';

USE WAREHOUSE MIGRATION_WH;

-- Create Database and Schema
CREATE DATABASE IF NOT EXISTS ENTERPRISE_MIGRATION_DB;
USE DATABASE ENTERPRISE_MIGRATION_DB;

CREATE SCHEMA IF NOT EXISTS TRADES_SCHEMA;
USE SCHEMA TRADES_SCHEMA;

-- Create the final target table
CREATE TABLE IF NOT EXISTS TARGET_TRADES (
    trade_id VARCHAR(50) NOT NULL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    volume INT NOT NULL,
    price NUMBER(18, 2) NOT NULL,
    trade_type VARCHAR(10) NOT NULL,
    status VARCHAR(20) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    execution_date TIMESTAMP_NTZ NOT NULL,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Final reconciled legacy trade data';

-- Create an external stage for the legacy CSV drops
CREATE OR REPLACE STAGE LEGACY_CSV_STAGE
    URL = 's3://enterprise-data-lake-bucket/legacy-extracts/trades/'
    CREDENTIALS = (AWS_KEY_ID = 'XXXX' AWS_SECRET_KEY = 'XXXX')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
    COMMENT = 'External stage for legacy trade CSV ingestion';
