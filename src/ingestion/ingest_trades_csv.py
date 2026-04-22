import pandas as pd      
from sqlalchemy import text    
from src.common.db import get_engine
from src.common.config_loader import load_sources   


FILE_PATH = "data/sample/sample_trades.csv"

def load_csv():
    sources = load_sources()
    file_path = sources["sources"]["trades"]["path"]

def load_csv():
    df = pd.read_csv(FILE_PATH)
    print(f"Loaded {len(df)} records from CSV")
    return df     

def create_table(engine):
    create_schema_sql = "CREATE SCHEMA IF NOT EXISTS bronze;"
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS bronze.trades_raw (
        trade_id TEXT,
        trade_date DATE,
        trader_id TEXT,
        instrument_id TEXT,
        quantity NUMERIC,
        price NUMERIC,
        side TEXT,
        source_system TEXT,
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    with engine.begin() as conn:
        conn.execute(text(create_schema_sql))
        conn.execute(text(create_table_sql))
        
def load_to_db(df, engine):
    df.to_sql(
        "trades_raw",
        engine,
        schema="bronze",
        if_exists="replace",
        index=False
    )
    print("Data loaded to bronze.trades_raw")
    
def main():
    engine= get_engine()
    df= load_csv()
    create_table(engine)
    load_to_db(df, engine)
    
if __name__ == "__main__":
    main()
    