# BigQuery Connector Developer Notes

The BigQuery connector module has both unit tests and integration tests.
The integration tests require access to a BigQuery instance in Google Cloud seeded with TPCH data.
You can follow the steps below to be able to run the integration tests locally.

## Requirements

* Access to a Google Cloud account. You can create one by visiting [this link](https://console.cloud.google.com/freetrial).
  A free tier account is sufficient.
* A working [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) installation. You should be authenticated to
  the account and project you want to use for running the tests.

## Steps

* [Enable BigQuery in your Google Cloud account](https://console.cloud.google.com/flows/enableapi?apiid=bigquery).
* Build the project by following the instructions [here](../../README.md).
* Run Trino with the TPCH connector installed using Docker as `docker run --rm --name trino -it -p 8080:8080
  trinodb/trino:latest`.
* Run the script `plugin/trino-bigquery/bin/import-tpch-to-bigquery.sh` and pass your Google Cloud project id to it as
  an argument. e.g. `plugin/trino-bigquery/bin/import-tpch-to-bigquery.sh trino-bigquery-07` where `trino-bigquery-07`
  is your Google Cloud project id.
* [Create a service account](https://cloud.google.com/docs/authentication/getting-started) in Google Cloud with the
  **BigQuery Admin** role assigned.
* Get the base64 encoded text of the service account credentials file using `base64
  /path/to/service_account_credentials.json`.
* Set the VM option `bigquery.credentials-key` in the IntelliJ "Run Configuration" (or on the CLI if using Maven
  directly). It should look something like `-Dbigquery.credentials-key=base64-text`.
* Run any test of your choice.
