### Enterprise Data Platform Pipeline

This project implements a containerized enterprise-style data pipeline for trade data using Airflow, Python, SQL, PostgreSQL, and Docker. The pipeline follows a bronze–silver–gold architecture and supports raw data ingestion, data validation, reject handling, and dimensional warehouse loading.

#### Architecture

CSV Trade Data  
- Bronze Schema (raw ingestion)  
- Silver Schema (validated clean records)  
- DQ Schema (rejected records)  
- Gold Schema (fact and dimension tables)  
- Airflow DAG orchestration

#### Architecture Diagram

  <img width="2124" height="1797" alt="image" src="https://github.com/user-attachments/assets/c0ae4146-e924-4502-a9e2-836086f7c17b" />


#### Tech Stack

- Python
- SQL
- PostgreSQL
- Apache Airflow
- Docker

#### Key Features

- Raw trade ingestion into bronze layer
- SQL-based transformation from bronze to silver
- Reject handling for invalid records
- Gold-layer warehouse with fact and dimension tables
- Airflow orchestration for end-to-end execution

#### Sample Validation Results

- bronze.trades_raw: 9 rows
- silver.trades_clean: 5 rows
- dq.trades_rejects: 4 rows
- gold.fact_trade: 5 rows
