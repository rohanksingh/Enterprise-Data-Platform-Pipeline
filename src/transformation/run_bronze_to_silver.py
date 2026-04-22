from sqlalchemy import text   
from src.common.db import get_engine

def run_sql_file(path):
    engine = get_engine()
    with open(path, "r") as f:
        sql = f.read()
        
    
    with engine.connect() as conn:
        conn.execute(text(sql))
        # for stmt in sql.split(";"):
        #     if stmt.strip():
        #         conn.execute(text(stmt))
        # conn.commit()
        
def main():
    run_sql_file("sql/dml/bronze_to_silver.sql")
    print("Bronze -> Silver SQL pipeline executed.")
    
if __name__ == "__main__":
    main()
    
