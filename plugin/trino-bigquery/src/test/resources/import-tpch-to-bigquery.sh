#!/bin/bash

set -euo pipefail
cd "${BASH_SOURCE%/*}"

# TODO: Adjust these values as needed
trino_cli="<PATH_To_TRINO_CLI>"
target_project="<GOOGLE_CLOUD_PROJECT_ID>"

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
