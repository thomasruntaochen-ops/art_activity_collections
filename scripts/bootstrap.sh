#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f .env ]]; then
  cp .env.example .env
fi

echo "Environment file ready."
echo "Create DB and apply schema:"
echo "  mysql -u root -p -e 'CREATE DATABASE IF NOT EXISTS art_activity_collection;'"
echo "  mysql -u root -p art_activity_collection < db/schema.sql"
