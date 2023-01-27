# BigQuery Connector Developer Notes

The BigQuery connector module has both unit tests and integration tests.
The integration tests require access to a BigQuery instance in Google Cloud.
You can follow the steps below to be able to run the integration tests locally.

## Requirements

* Access to a Google Cloud account. You can create one by visiting [this link](https://console.cloud.google.com/freetrial).
  A free tier account is sufficient.
* A working [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) installation. You should be authenticated to
  the account and project you want to use for running the tests.

## Steps

* [Enable BigQuery in your Google Cloud account](https://console.cloud.google.com/flows/enableapi?apiid=bigquery).
* Build the project by following the instructions [here](../../README.md).
* Create a Google Cloud Storage bucket using `gsutil mb gs://DESTINATION_BUCKET_NAME`
* Run `gsutil cp plugin/trino-bigquery/src/test/resources/region.csv gs://DESTINATION_BUCKET_NAME/tpch/tiny/region.csv` 
  (replace `DESTINATION_BUCKET_NAME` with the target bucket name).
* [Create a service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console) in Google Cloud with the
  **BigQuery Admin** role assigned.
* Get the base64 encoded text of the service account credentials file using `base64
  /path/to/service_account_credentials.json`.
* The `TestBigQueryWithDifferentProjectIdConnectorSmokeTest` requires an alternate project ID which is different from the
  project ID attached to the service account but the service account still has access to.
* Set the VM options `bigquery.credentials-key`, `testing.gcp-storage-bucket`, and `testing.alternate-bq-project-id` in the IntelliJ "Run Configuration"
  (or on the CLI if using Maven directly). It should look something like
  `-Dbigquery.credentials-key=base64-text -Dtesting.gcp-storage-bucket=DESTINATION_BUCKET_NAME -Dtesting.alternate-bq-project-id=bigquery-cicd-alternate`.
* Run any test of your choice.
