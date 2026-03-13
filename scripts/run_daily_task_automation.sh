#!/bin/zsh

set -euo pipefail

REPO_DIR="/Users/christopherstorer/github/personal/daily-task-automation"
PYTHON_BIN="$REPO_DIR/.venv/bin/python"
SCRIPT_PATH="$REPO_DIR/main.py"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Expected virtualenv Python at $PYTHON_BIN"
  exit 1
fi

cd "$REPO_DIR"
exec "$PYTHON_BIN" "$SCRIPT_PATH"