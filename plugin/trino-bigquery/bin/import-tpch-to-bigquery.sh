#!/bin/bash

set -euo pipefail
cd "${BASH_SOURCE%/*}"

if [[ "$#" -ne 1 ]]; then
  printf '%s\n' "Usage: $0 <google_cloud_project_id>"
  exit 1
fi

target_project="$1"
trino_cli="../../../client/trino-cli/target/trino-cli-*-executable.jar"

for file in $trino_cli; do
  if [[ -x "$file" ]]; then
    trino_cli="$file"
    break
  else
    printf '%s\n' "Couldn't find $trino_cli. Please build the project first following the steps in the README."
    exit 1
  fi
done

source_catalog=tpch
source_schema=tiny

tables=('customer' 'lineitem' 'nation' 'orders' 'part' 'partsupp' 'region' 'supplier')

function trino_cli() {
  ${trino_cli} "$@"
}

printf '%s\n' "Creating datasets... You can safely ignore any messages about a dataset already existing."
bq mk --dataset --description 'TPCH dataset for Trino tests' "$target_project:tpch" || true
bq mk --dataset --description 'Test dataset for Trino tests' "$target_project:test" || true

for table in "${tables[@]}"; do
  printf '%s\n' "Exporting ${source_catalog}.${source_schema}.${table}"
  trino_cli --execute "SELECT * FROM ${source_catalog}.${source_schema}.${table}" --output-format=CSV > "/tmp/${table}.csv"
  printf '%s\n' "Importing ${table}.csv to tpch.${table}"
  # if the tables already exist they will be overwritten due to --replace
  bq --project_id="${target_project}" load --replace --source_format=CSV "tpch.${table}" "/tmp/${table}.csv" "./schema/${table}.json"
done
