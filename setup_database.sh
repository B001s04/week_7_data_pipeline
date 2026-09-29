#!/usr/bin/env bash
# setup_database.sh - creates the pipeline tables and the Airflow connection lemmy_db.
# Works with any PostgreSQL server you can reach (no sudo needed).
# Usage (from the project folder, after setup_airflow.sh):  bash setup_database.sh
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "Enter your database details (press Enter to accept the value in [brackets])."
read -rp "Host [localhost]: " DB_HOST;   DB_HOST=${DB_HOST:-localhost}
read -rp "Port [5432]: " DB_PORT;        DB_PORT=${DB_PORT:-5432}
read -rp "Database name [LEMMY]: " DB_NAME; DB_NAME=${DB_NAME:-LEMMY}
read -rp "Username [lemmy_user]: " DB_USER; DB_USER=${DB_USER:-lemmy_user}
read -rsp "Password (hidden while typing): " DB_PASS; echo
echo "If several students share this database, use your own schema (e.g. your student ID)."
read -rp "Schema [public]: " DB_SCHEMA;  DB_SCHEMA=${DB_SCHEMA:-public}
DB_SCHEMA=$(echo "$DB_SCHEMA" | tr '[:upper:]' '[:lower:]')

export PGHOST="$DB_HOST" PGPORT="$DB_PORT" PGDATABASE="$DB_NAME" PGUSER="$DB_USER" PGPASSWORD="$DB_PASS"

echo ">>> Testing the connection ..."
psql -qAtc "select 'Connected to ' || current_database() || ' as ' || current_user"

if [ "$DB_SCHEMA" != "public" ]; then
  echo ">>> Creating schema $DB_SCHEMA ..."
  psql -qc "CREATE SCHEMA IF NOT EXISTS \"$DB_SCHEMA\";"
fi
export PGOPTIONS="-c search_path=$DB_SCHEMA"

echo ">>> Creating tables ..."
psql -q -v ON_ERROR_STOP=1 -f dags/sql/00_create_tables.sql 2>&1 | grep -v 'already exists, skipping' || true
psql -q -v ON_ERROR_STOP=1 -f dags/sql/01_qc_checks.sql
psql -c "\dt"

echo ">>> Creating the Airflow connection lemmy_db ..."
source env.sh >/dev/null
airflow db migrate >/dev/null
airflow connections delete lemmy_db >/dev/null 2>&1 || true
EXTRA="{}"
[ "$DB_SCHEMA" != "public" ] && EXTRA="{\"options\": \"-c search_path=$DB_SCHEMA\"}"
airflow connections add lemmy_db --conn-type postgres --conn-host "$DB_HOST" --conn-port "$DB_PORT" \
  --conn-schema "$DB_NAME" --conn-login "$DB_USER" --conn-password "$DB_PASS" --conn-extra "$EXTRA" >/dev/null
echo ">>> Connection lemmy_db saved."

# Remember the settings (not the password) so psql works later with: source db.sh
cat > db.sh <<EOT
export PGHOST="$DB_HOST" PGPORT="$DB_PORT" PGDATABASE="$DB_NAME" PGUSER="$DB_USER"
export PGOPTIONS="-c search_path=$DB_SCHEMA"
echo "psql will now connect to $DB_NAME on $DB_HOST (schema $DB_SCHEMA). It will ask for the password."
EOT
echo ">>> Done. Next: source env.sh  then  airflow standalone"
