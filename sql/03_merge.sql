/*
    Snowflake Merge Script
    Idempotent MERGE operation to enforce zero-duplicate loads into the final target table.
*/

USE WAREHOUSE MIGRATION_WH;
USE DATABASE ENTERPRISE_MIGRATION_DB;
USE SCHEMA TRADES_SCHEMA;

MERGE INTO TARGET_TRADES AS tgt
USING STG_TRADES AS src
ON tgt.trade_id = src.trade_id
WHEN MATCHED THEN
    UPDATE SET 
        tgt.symbol = src.symbol,
        tgt.volume = src.volume,
        tgt.price = src.price,
        tgt.trade_type = src.trade_type,
        tgt.status = src.status,
        tgt.exchange = src.exchange,
        tgt.execution_date = src.execution_date,
        tgt.loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        trade_id, 
        symbol, 
        volume, 
        price, 
        trade_type, 
        status, 
        exchange, 
        execution_date, 
        loaded_at
    )
    VALUES (
        src.trade_id, 
        src.symbol, 
        src.volume, 
        src.price, 
        src.trade_type, 
        src.status, 
        src.exchange, 
        src.execution_date, 
        CURRENT_TIMESTAMP()
    );
