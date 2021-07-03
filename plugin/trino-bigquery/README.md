# BigQuery Connector Developer Notes

The BigQuery connector module has both some unit tests and some integration tests.
The integration tests require access to a BigQuery instance in Google Cloud seeded with TPCH data.
You can follow the steps below to be able to run the integration tests locally.

## Requirements

* Access to a Google Cloud account.
* A working [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) installation. You should be authenticated to
  the account and project you want to use for running the tests.

## Steps

* [Enable BigQuery in your Google Cloud account](https://console.cloud.google.com/flows/enableapi?apiid=bigquery).
* Run Trino with the TPCH connector installed using Docker as `docker run --rm --name trino -it -p 8080:8080
  trinodb/trino:latest`.
* Run the below script after adjusting values according to your Google Cloud details.

  ```shell
  #!/bin/bash

  set -euo pipefail

  # TODO: Adjust these values as needed
  trino_cli="<PATH_TO_TRINO_CLI>"
  target_project="<GOOGLE_CLOUD_PROJECT_ID>"

  source_catalog=tpch
  source_schema=tiny

  tables=('customer' 'lineitem' 'nation' 'orders' 'part' 'partsupp' 'region' 'supplier')
  # field:type,field:type,...
  table_schemas=(
  'custkey:INTEGER,name:STRING,address:STRING,nationkey:INTEGER,phone:STRING,acctbal:FLOAT,mktsegment:STRING,comment:STRING'
  'orderkey:INTEGER,partkey:INTEGER,suppkey:INTEGER,linenumber:INTEGER,quantity:FLOAT,extendedprice:FLOAT,discount:FLOAT,tax:FLOAT,returnflag:STRING,linestatus:STRING,shipdate:DATE,commitdate:DATE,receiptdate:DATE,shipinstruct:STRING,shipmode:STRING,comment:STRING'
  'nationkey:INTEGER,name:STRING,regionkey:INTEGER,comment:STRING'
  'orderkey:INTEGER,custkey:INTEGER,orderstatus:STRING,totalprice:FLOAT,orderdate:DATE,orderpriority:STRING,clerk:STRING,shippriority:INTEGER,comment:STRING'
  'partkey:INTEGER,name:STRING,mfgr:STRING,brand:STRING,type:STRING,size:INTEGER,container:STRING,retailprice:FLOAT,comment:STRING'
  'partkey:INTEGER,suppkey:INTEGER,availqty:INTEGER,supplycost:FLOAT,comment:STRING'
  'regionkey:INTEGER,name:STRING,comment:STRING'
  'suppkey:INTEGER,name:STRING,address:STRING,nationkey:INTEGER,phone:STRING,acctbal:FLOAT,comment:STRING'
  )

  function trino_cli() {
    ${trino_cli} "$@"
  }

  printf '%s\n' "Creating datasets... You can safely ignore any messages about a dataset already existing."
  bq mk --dataset --description 'TPCH dataset for Trino tests' "$target_project:tpch" || true
  bq mk --dataset --description 'Test dataset for Trino tests' "$target_project:test" || true

  for i in "${!tables[@]}"; do
    table="${tables[i]}"
    schema="${table_schemas[i]}"
    printf '%s\n' "Exporting ${source_catalog}.${source_schema}.${table}"
    trino_cli --execute "SELECT * FROM ${source_catalog}.${source_schema}.${table}" --output-format=CSV > "${table}.csv"
    printf '%s\n' "Importing ${table}.csv to tpch.${table}"
    # if the tables already exist they will be overwritten due to --replace
    bq --project_id="${target_project}" load --replace --source_format=CSV "tpch.${table}" "${table}.csv" "${schema}"
  done
  ```
* [Create a service account](https://cloud.google.com/docs/authentication/getting-started) in Google Cloud with the
  **BigQuery Admin** role assigned.
* Get the base64 encoded text of the service account credentials file using `base64
  /path/to/service_account_credentials.json`.
* Set the VM option `bigquery.credentials-key` in the IntelliJ "Run Configuration" (or on the CLI if using Maven
  directly). It should look something like `-Dbigquery.credentials-key=base64-text`.
* Run any test of your choice.

**NOTE: Once you are done with running tests don't forget to delete the datasets created in BigQuery to prevent
incurring charges once you are done.**
