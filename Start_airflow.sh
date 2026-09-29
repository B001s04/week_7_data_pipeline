#!/usr/bin/env bash
# start_airflow.sh - starts Airflow on a free port (8081 or higher).
# Usage (from the project folder):  bash start_airflow.sh
cd "$(dirname "${BASH_SOURCE[0]}")"

# Remove any mistyped port lines from env.sh
sed -i '/WEBSERVER-PORT/d; /AIRFLOW__WEBSERVER_PORT/d; /AIRFLOW__WEBSERVER__WEB_SERVER_PORT/d' env.sh

source env.sh

# Start the local PostgreSQL if it is installed and not running
[ -f local_postgres.sh ] && bash local_postgres.sh start 2>/dev/null | grep -v "already running" || true

# Find a free port, starting at 8081
PORT=8081
while (echo > "/dev/tcp/127.0.0.1/$PORT") >/dev/null 2>&1; do PORT=$((PORT + 1)); done
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=$PORT

echo "=============================================================="
echo " Airflow web page:  http://localhost:$PORT"
echo " Username: admin"
echo " Password: $(cat airflow_home/standalone_admin_password.txt 2>/dev/null || echo 'shown below when Airflow is ready')"
echo " Keep this terminal open. Stop Airflow with Ctrl+C."
echo "=============================================================="
exec airflow standalone
