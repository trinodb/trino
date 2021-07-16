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
* Run the script `plugin/trino-bigquery/src/test/resources/import-tpch-to-bigquery.sh` after adjusting values according
  to your Google Cloud details.
* [Create a service account](https://cloud.google.com/docs/authentication/getting-started) in Google Cloud with the
  **BigQuery Admin** role assigned.
* Get the base64 encoded text of the service account credentials file using `base64
  /path/to/service_account_credentials.json`.
* Set the VM option `bigquery.credentials-key` in the IntelliJ "Run Configuration" (or on the CLI if using Maven
  directly). It should look something like `-Dbigquery.credentials-key=base64-text`.
* Run any test of your choice.

**NOTE: Once you are done with running tests don't forget to delete the datasets created in BigQuery to prevent
incurring charges once you are done.**
