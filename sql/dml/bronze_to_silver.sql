-- Create schemas 
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS dq;

-- Create tables
CREATE TABLE IF NOT EXISTS silver.trades_clean (
trade_id TEXT,
trade_date DATE,
trader_id TEXT,
instrument_id TEXT,
quantity NUMERIC,
price NUMERIC,
side TEXT,
source_system TEXT,
notional NUMERIC,
load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

CREATE TABLE IF NOT EXISTS dq.trades_rejects (
trade_id TEXT,
trade_date TEXT,
trader_id TEXT,
instrument_id TEXT,
quantity TEXT,
price TEXT,
side TEXT,
source_system TEXT,
reject_reason TEXT,
reject_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert valid rows
INSERT INTO silver.trades_clean (
trade_id,
trade_date,
trader_id,
instrument_id,
quantity,
price,
side,
source_system,
notional
)
SELECT 
trade_id,
CAST(trade_date AS DATE),
trader_id,
instrument_id,
CAST(quantity AS NUMERIC),
CAST(price AS NUMERIC),
side,
source_system,
CAST(quantity AS NUMERIC) * CAST(price AS NUMERIC) AS notional
FROM bronze.trades_raw
WHERE 
trade_id IS NOT NULL
AND trade_date IS NOT NULL
AND instrument_id IS NOT NULL
AND CAST(quantity AS NUMERIC) > 0
AND CAST(price AS NUMERIC) > 0
AND side IN ('BUY', 'SELL');

-- Insert rejected rows
INSERT INTO dq.trades_rejects (
trade_id,
trade_date,
trader_id,
instrument_id,
quantity,
price,
side,
source_system,
reject_reason
)
SELECT 
trade_id,
trade_date,
trader_id,
instrument_id,
quantity,
price,
side,
source_system,
CONCAT(
CASE WHEN trade_id IS NULL THEN 'missing_trade_id;' ELSE '' END,
CASE WHEN trade_date IS NULL THEN 'missing_trade_date;' ELSE '' END,
CASE WHEN trader_id IS NULL THEN 'missing_trader_id;' ELSE '' END,
CASE WHEN instrument_id IS NULL THEN 'missing_instrument_id;' ELSE '' END,
CASE WHEN CAST(quantity AS NUMERIC) <=0 THEN 'invalid_quantity;' ELSE '' END,
CASE WHEN CAST(price AS NUMERIC) <=0 THEN 'invalid_price;' ELSE '' END,
CASE WHEN side NOT IN ('BUY', 'SELL') THEN 'invalid_side;' ELSE '' END
)
FROM bronze.trades_raw
WHERE NOT (
trade_id IS NOT NULL 
AND trade_date IS NOT NULL 
AND trader_id IS NOT NULL
AND instrument_id IS NOT NULL
AND CAST(quantity AS NUMERIC) > 0
AND CAST(price AS NUMERIC) > 0
AND side IN ('BUY', 'SELL')
);

-- audit table
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name TEXT,
    task_name TEXT,
    status TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INTEGER,
    error_message TEXT
);








