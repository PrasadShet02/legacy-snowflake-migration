/*
    Snowflake Load Transient Script
    Creates a TRANSIENT staging table for cost-efficiency and loads data via COPY INTO.
*/

USE WAREHOUSE MIGRATION_WH;
USE DATABASE ENTERPRISE_MIGRATION_DB;
USE SCHEMA TRADES_SCHEMA;

-- Transient table saves on Time Travel and Fail-safe storage costs for intermediate data
CREATE TRANSIENT TABLE IF NOT EXISTS STG_TRADES (
    trade_id VARCHAR(50),
    symbol VARCHAR(20),
    volume INT,
    price NUMBER(18, 2),
    trade_type VARCHAR(10),
    status VARCHAR(20),
    exchange VARCHAR(20),
    execution_date TIMESTAMP_NTZ
)
COMMENT = 'Cost-optimized transient staging table for legacy trade drops';

-- Truncate staging table in case of re-runs
TRUNCATE TABLE STG_TRADES;

-- Load the latest CSV file from the external stage into the transient table
COPY INTO STG_TRADES
FROM @LEGACY_CSV_STAGE
ON_ERROR = 'CONTINUE'
PURGE = FALSE;
