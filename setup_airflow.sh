#!/usr/bin/env bash
# setup_airflow.sh - Part 5 of the guide in one go (no sudo needed).
# Usage (from the project folder):  bash setup_airflow.sh
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"
deactivate 2>/dev/null || true

if ! command -v uv >/dev/null 2>&1 && [ ! -x "$HOME/.local/bin/uv" ]; then
  echo ">>> Installing uv ..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="$HOME/.local/bin:$PATH"
uv --version

echo ">>> Creating .venv with Python 3.12 ..."
rm -rf .venv
uv venv --python 3.12 .venv

echo ">>> Installing Airflow 2.9.2 (takes a few minutes) ..."
uv pip install --python .venv/bin/python -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt"

source env.sh
echo ">>> Airflow version: $(airflow version 2>/dev/null | tail -1)"
echo ">>> Done. Next: bash setup_database.sh"
