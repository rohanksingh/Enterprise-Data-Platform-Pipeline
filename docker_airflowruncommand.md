#  Enterprise Data Platform - Command Runbook

---

##  0. Navigate to Project

```bash
cd C:\Users\rohan\enterprise_data_platform_pipeline
```

---

##  1. Full Reset (Clean Start)

 Use only when things break or config changes

```bash
docker compose down -v --remove-orphans
```

---

##  2. Start PostgreSQL Only

```bash
docker compose up -d postgres
```

---

##  3. Initialize Airflow Database

```bash
docker compose run --rm airflow-init airflow db migrate
```

---

##  4. Create Airflow Admin User

```bash
docker compose run --rm airflow-init airflow users create \
  --username admin \
  --password admin \
  --firstname admin \
  --lastname user \
  --role Admin \
  --email admin@example.com
```

---

##  5. Start Full Platform

```bash
docker compose up -d
```

---

##  6. Check Running Containers

```bash
docker ps
```

---

##  7. Check Logs (if something breaks)

```bash
docker compose logs airflow-webserver
docker compose logs airflow-scheduler
docker compose logs airflow-init
```

---

##  8. Access Airflow UI

Open browser:

```text
http://localhost:8080
```

Login:

* Username: admin
* Password: admin

---

##  9. Stop Services (without deleting data)

```bash
docker compose down
```

---

##  10. Run Ingestion Pipeline (Local)

```bash
python -m src.ingestion.ingest_trades_csv
```

---

##  11. Verify Data in PostgreSQL

```bash
psql -h localhost -p 5432 -U admin -d enterprise_data
```

Then:

```sql
SELECT * FROM bronze.trades_raw;
```

---

#  Notes

* `down -v` → deletes database (use carefully)
* `run airflow-init` → runs one-time setup commands
* Airflow runs inside Docker (NOT local Python)
* DAG files go in: `airflow/dags/`

---

#  Common Issues

###  Airflow UI not loading

→ Check:

```bash
docker ps
docker compose logs airflow-webserver
```

###  DB connection failed

→ Check:

* `.env`
* port (5432 vs 5433)
* credentials consistency

###  No DAGs showing

→ Ensure file exists in:

```text
airflow/dags/
```

---

#  Next Step

Build and run:

* trade_ingestion_pipeline DAG
* then bronze → silver transformation

---
