#!/usr/bin/env bash
# local_postgres.sh - run your own PostgreSQL server WITHOUT sudo.
#
# PostgreSQL 16 is installed as a Python package (pgserver) into your home folder,
# and the server runs as your own user. Nothing system-wide is changed.
#
#   bash local_postgres.sh install     # once: install, create lemmy_user + LEMMY, start it
#   bash local_postgres.sh start       # after every restart of the machine
#   bash local_postgres.sh stop
#   bash local_postgres.sh status
#   bash local_postgres.sh autostart   # optional: start it automatically at boot (uses cron)
set -e

PG_VENV="$HOME/.local/pgserver-venv"   # where the PostgreSQL program is installed
PGDATA_DIR="$HOME/pgdata"              # where your databases are stored
PORT_FILE="$PGDATA_DIR/.port"
DB_USER="lemmy_user"
DB_NAME="LEMMY"

find_bin() {
    local ctl
    ctl=$(find "$PG_VENV" -path "*pgserver*" -name pg_ctl -type f 2>/dev/null | head -1)
    [ -n "$ctl" ] && BIN=$(dirname "$ctl")
}
port() { cat "$PORT_FILE" 2>/dev/null || echo 5432; }
port_in_use() { (echo > "/dev/tcp/127.0.0.1/$1") >/dev/null 2>&1; }
is_running() { [ -n "$BIN" ] && "$BIN/pg_ctl" -D "$PGDATA_DIR" status >/dev/null 2>&1; }

start_server() {
    if is_running; then echo "PostgreSQL is already running on port $(port)."; return; fi
    "$BIN/pg_ctl" -D "$PGDATA_DIR" -l "$PGDATA_DIR/server.log" -w \
        -o "-p $(port) -k $PGDATA_DIR -c listen_addresses=localhost" start >/dev/null
    echo "PostgreSQL started on localhost, port $(port)."
}

cmd="${1:-status}"
find_bin || true

case "$cmd" in
install)
    if [ -f "$PGDATA_DIR/PG_VERSION" ]; then
        echo "Already installed in $PGDATA_DIR."; start_server; exit 0
    fi
    export PATH="$HOME/.local/bin:$PATH"
    if ! command -v uv >/dev/null 2>&1; then
        echo ">>> Installing uv ..."; curl -LsSf https://astral.sh/uv/install.sh | sh
    fi
    echo ">>> Installing PostgreSQL 16 (as a Python package, no sudo) ..."
    uv venv -q --clear --python 3.12 "$PG_VENV"
    uv pip install -q --python "$PG_VENV/bin/python" pgserver
    find_bin || { echo "ERROR: PostgreSQL files not found after install."; exit 1; }

    echo
    echo "Choose a password for the database user $DB_USER (letters and numbers only)."
    while true; do
        read -rsp "Password: " P1; echo
        read -rsp "Repeat password: " P2; echo
        [ -n "$P1" ] && [ "$P1" = "$P2" ] && break
        echo "The passwords were empty or did not match - try again."
    done

    PORT=5432
    if port_in_use 5432; then PORT=5433; echo "Port 5432 is busy, using port 5433 instead."; fi

    echo ">>> Creating the database cluster in $PGDATA_DIR ..."
    PWFILE=$(mktemp); printf '%s\n' "$P1" > "$PWFILE"
    "$BIN/initdb" -D "$PGDATA_DIR" -U "$DB_USER" --pwfile="$PWFILE" \
        --auth=scram-sha-256 -E UTF8 --no-locale >/dev/null
    rm -f "$PWFILE"
    echo "$PORT" > "$PORT_FILE"

    start_server
    PGPASSWORD="$P1" "$BIN/createdb" -h localhost -p "$PORT" -U "$DB_USER" "$DB_NAME"
    echo ">>> Created database $DB_NAME owned by $DB_USER."
    echo
    echo "Done. Your connection details:"
    echo "   Host: localhost   Port: $PORT   Database: $DB_NAME   User: $DB_USER"
    echo "Next: bash setup_airflow.sh   then   bash setup_database.sh"
    ;;
start)
    [ -n "$BIN" ] && [ -f "$PGDATA_DIR/PG_VERSION" ] || { echo "Not installed yet: bash local_postgres.sh install"; exit 1; }
    start_server
    ;;
stop)
    if is_running; then "$BIN/pg_ctl" -D "$PGDATA_DIR" -m fast stop >/dev/null; echo "PostgreSQL stopped."
    else echo "PostgreSQL is not running."; fi
    ;;
status)
    if [ -z "$BIN" ] || [ ! -f "$PGDATA_DIR/PG_VERSION" ]; then echo "Not installed yet: bash local_postgres.sh install"
    elif is_running; then echo "online - PostgreSQL is running on localhost, port $(port)."
    else echo "down - start it with: bash local_postgres.sh start"; fi
    ;;
autostart)
    SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
    LINE="@reboot bash $SCRIPT start >> $PGDATA_DIR/autostart.log 2>&1"
    if crontab -l 2>/dev/null | grep -qF "$SCRIPT start"; then echo "Autostart is already set up."
    else (crontab -l 2>/dev/null; echo "$LINE") | crontab - && echo "PostgreSQL will now start automatically at boot."
    fi
    ;;
*)
    echo "Usage: bash local_postgres.sh install | start | stop | status | autostart"; exit 1
    ;;
esac
