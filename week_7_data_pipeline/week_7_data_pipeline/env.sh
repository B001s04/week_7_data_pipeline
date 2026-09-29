# env.sh - Airflow settings for the Lemmy pipeline.
# Use it with:   source env.sh      (run it in every new terminal)

# The folder this file lives in (the project folder)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Keep all of Airflow's own files inside the project, and read DAGs from dags/
export AIRFLOW_HOME="$PROJECT_DIR/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
export AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False

# Switch off any other Python environment that is already active
if [ -n "$VIRTUAL_ENV" ] && [ "$VIRTUAL_ENV" != "$PROJECT_DIR/.venv" ]; then
    deactivate 2>/dev/null || true
fi

# Activate the project's Python environment (created in Part 5 of the guide)
if [ -f "$PROJECT_DIR/.venv/bin/activate" ]; then
    source "$PROJECT_DIR/.venv/bin/activate"
    echo "Lemmy pipeline environment ready (AIRFLOW_HOME=$AIRFLOW_HOME)"
else
    echo "WARNING: $PROJECT_DIR/.venv not found - create it first (guide Part 5.2)."
fi
