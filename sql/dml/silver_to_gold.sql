CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_trader (
trader_key SERIAL PRIMARY KEY,
trader_id TEXT UNIQUE 
);

CREATE TABLE IF NOT EXISTS gold.dim_instrument (
instrument_key SERIAL PRIMARY KEY ,
instrument_id TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
date_key INTEGER PRIMARY KEY,
full_date DATE UNIQUE,
year INTEGER,
month INTEGER,
day INTEGER
);

CREATE TABLE IF NOT EXISTS gold.fact_trade (
trade_id TEXT PRIMARY KEY,
date_key INTEGER,
trader_key INTEGER,
instrument_key INTEGER,
quantity NUMERIC,
price NUMERIC,
notional NUMERIC,
side TEXT,
source_system TEXT
);

TRUNCATE TABLE gold.fact_trade RESTART IDENTITY CASCADE;     -- mysql syntax:TRUNCATE TABLE gold.fact_trade;
TRUNCATE TABLE gold.dim_trader RESTART IDENTITY CASCADE;
TRUNCATE TABLE gold.dim_instrument RESTART IDENTITY CASCADE;
TRUNCATE TABLE gold.dim_date RESTART IDENTITY CASCADE;

INSERT INTO gold.dim_trader (trader_id)
SELECT DISTINCT trader_id 
FROM silver.trades_clean
WHERE trader_id IS NOT NULL;

INSERT INTO gold.dim_instrument (instrument_id)
SELECT DISTINCT instrument_id
FROM silver.trades_clean 
WHERE instrument_id IS NOT NULL;

INSERT INTO gold.dim_date (date_key, full_date, year, month, day)
SELECT DISTINCT 
	CAST(TO_CHAR(trade_date, 'YYYYMMDD') AS INTEGER) AS date_key,
	trade_date AS full_date,
	EXTRACT(YEAR FROM trade_date)::INTEGER AS year,
	EXTRACT(MONTH FROM trade_date)::INTEGER AS month,
	EXTRACT(DAY FROM trade_date)::INTEGER AS day
FROM silver.trades_clean
WHERE trade_date IS NOT NULL;

INSERT INTO gold.fact_trade (
trade_id,
date_key,
trader_key,
instrument_key,
quantity,
price,
notional,
side,
source_system
)
SELECT 
s.trade_id,
CAST(TO_CHAR(s.trade_date, 'YYYYMMDD') AS INTEGER) AS date_key,
dt.trader_key,
di.instrument_key,
s.quantity,
s.price,
s.notional,
s.side,
s.source_system
FROM silver.trades_clean s
JOIN gold.dim_trader dt 
ON s.trader_id = dt.trader_id
JOIN gold.dim_instrument di 
ON s.instrument_id = di.instrument_id;




