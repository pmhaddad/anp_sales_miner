#!/bin/sh
set -e

echo "Welcome to ANP sales data miner!"
pipenv run --quiet airflow db migrate

exec "$@"
