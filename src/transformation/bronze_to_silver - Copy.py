import pandas as pd     
from sqlalchemy import text  
from src.common.db import get_engine   

def create_target_tables(engine):
    ddl_statements = [
        "CREATE SCHEMA IF NOT EXISTS silver",
        "CREATE SCHEMA IF NOT EXISTS dq",
        "DROP TABLE IF EXISTS silver.trades_clean",
    """
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
    )
    """
    ,
    """
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
    )
    """
    ]
    
    with engine.connect() as conn:
        for stmt in ddl_statements:
            conn.execute(text(stmt))
        conn.commit()
        
def extract_bronze(engine):
    query = "SELECT trade_id, trade_date, trader_id, instrument_id, quantity, price, side, source_system FROM bronze.trades_raw" 
    return pd.read_sql(query, engine)

def validate_and_split(df: pd.DataFrame):
    df= df.copy()
    df["reject_reason"] = ""
    
    df.loc[df["trade_id"].isna(), "reject_reason"] += "missing_trade_id;"
    df.loc[df["trade_date"].isna(), "reject_reason"] += "missing_trade_date;"
    df.loc[df["trader_id"].isna(), "reject_reason"] += "missing_trader_id;"
    df.loc[df["instrument_id"].isna(), "reject_reason"] += "missing_instrument_id;"
    df.loc[df["quantity"].isna() | (pd.to_numeric(df["quantity"], errors="coerce") <= 0), "reject_reason"] += "invalid_quantity;"
    df.loc[df["price"].isna() | (pd.to_numeric(df["price"], errors="coerce") <= 0), "reject_reason"] += "invalid_price;"
    df.loc[~df["side"].isin(["BUY", "SELL"]), "reject_reason"] += "invalid_side;"
    
    valid_df = df[df["reject_reason"] == ""].copy()
    # print(valid_df)
    reject_df = df[df["reject_reason"]!= ""].copy()
    # print(reject_df)
    
    valid_df["quantity"] = pd.to_numeric(valid_df["quantity"], errors="coerce")

    valid_df["price"] = pd.to_numeric(valid_df["price"], errors="coerce")

    valid_df["trade_date"] = pd.to_datetime(valid_df["trade_date"], errors="coerce").dt.date

    valid_df["notional"] = (valid_df["quantity"] * valid_df["price"]).round(2)
    print(valid_df)
    
    valid_df = valid_df[["trade_id", "trade_date", "trader_id", "instrument_id", "quantity", "price", "side", "source_system", "notional"]]
    reject_df = reject_df[["trade_id", "trade_date", "trader_id", "instrument_id", "quantity", "price", "side", "source_system", "reject_reason"]]
    # print(valid_df)
    
    return valid_df, reject_df

def load_outputs(valid_df, reject_df, engine):
    if not valid_df.empty:
        valid_df.to_sql(
            "trades_clean",
            engine,
            schema="silver",
            if_exists="append",
            index=False
        )
        
    if not reject_df.empty:
        reject_df.to_sql(
            "trades_rejects",
            engine,
            schema="dq",
            if_exists="append",
            index=False
        )
        
def main():
    engine = get_engine()
    create_target_tables(engine)
    bronze_df = extract_bronze(engine)
    valid_df, reject_df = validate_and_split(bronze_df)
    load_outputs(valid_df, reject_df, engine)
    
    print(f"Valid rows loaded: {len(valid_df)}")
    print(f"Rejected rows loaded: {len(reject_df)}")
    
if __name__ == "__main__":
    main()
        
    
        