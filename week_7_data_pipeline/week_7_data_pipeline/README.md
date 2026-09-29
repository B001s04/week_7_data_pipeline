# week_7_data_pipeline — Lemmy Data Pipeline

An Apache Airflow DAG (`lemmy_pipeline`) that fetches the hottest posts from the
Lemmy community **c/books** on lemmy.world and loads them into the PostgreSQL
database **LEMMY**.

```
lemmy_api_call → load_stage_data → load_dice_data → qc_check → update_control_table
```

## Folder contents

```
week_7_data_pipeline/
├── dags/
│   ├── lemmy_pipeline.py        the DAG (all five tasks)
│   └── sql/
│       ├── 00_create_tables.sql  creates stage, dice and control tables
│       ├── 01_qc_checks.sql      creates qc_checks with LEMMY_COUNT_CHECK
│       ├── 02_load_stage.sql     insert one raw post into stage
│       ├── 03_stage_to_dice.sql  upsert this run's posts into dice
│       ├── 04_qc_check.sql       list the active QC checks
│       └── 05_update_ctl.sql     write the run's audit row
├── env.sh                         environment settings for Airflow
├── requirements.txt               Python packages
└── README.md
```

## Quick start (full steps are in the setup guide)

```bash
# 1. Database (once)
sudo -u postgres psql -c "CREATE ROLE lemmy_user WITH LOGIN SUPERUSER PASSWORD 'YOUR_DB_PASSWORD';"
sudo -u postgres createdb -O lemmy_user LEMMY
export PGPASSWORD='YOUR_DB_PASSWORD'
psql -h localhost -U lemmy_user -d LEMMY -f dags/sql/00_create_tables.sql
psql -h localhost -U lemmy_user -d LEMMY -f dags/sql/01_qc_checks.sql

# 2. Python + Airflow (once)
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt"

# 3. Start Airflow (every time)
source env.sh
airflow standalone

# 4. Connection (once, in a second terminal)
source env.sh
airflow connections add lemmy_db --conn-uri "postgresql://lemmy_user:YOUR_DB_PASSWORD@localhost:5432/LEMMY"
```

Then open http://localhost:8080, unpause **lemmy_pipeline** and click **Trigger DAG**.

Never commit passwords: the database password lives only in the Airflow connection,
not in any file in this repository.
