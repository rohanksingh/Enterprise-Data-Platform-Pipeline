from sqlalchemy import text      
from src.common.db import get_engine    

def run_sql_file(path):
    engine = get_engine()
    
    with open(path, "r") as f:
        sql = f.read()
        
    with engine.connect() as conn:
        conn.execute(text(sql))
        # conn.commit()
        
def main():
    run_sql_file("sql/dml/silver_to_gold.sql")
    print("Silver -> Gold SQL pipeline executed.")
    
if __name__ == "__main__":
    main()
    
    