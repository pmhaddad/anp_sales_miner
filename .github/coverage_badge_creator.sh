#!/bin/bash

coverage_perc="$(pipenv run coverage json -o /dev/stdout | jq '.totals.percent_covered_display | tonumber')"

if [ "$coverage_perc" -ge 85 ]; then
  color="green"
elif [ "$coverage_perc" -ge 65 ]; then
  color="yellow"
else
  color="red"
fi

curl "https://img.shields.io/badge/coverage-$coverage_perc%25-$color" -o coverage_badge.svg --no-progress-meter
