# Setting up google sheets plugin

## How to generate google-sheets service account credentials
1. Open https://console.developers.google.com/apis/library and search "google sheets api"

2. Enable and create a project, it will take you to API Manager page, please click on Credentials in left panel.

3. Under credentials page, create a service account and generate a service account json key
      and save the key as `/usr/local/presto/credentials/google-sheets-credentials.json`.
      
4. Provide the path of credential in `gsheets.properties` as `credentials-path`.

## Setting up metadata sheet

1. Now create a metadata sheet to keep table_name, sheet id mapping

2. Share this metadata sheet with service account user (present in the credentials file).

3. Provide the id of this sheet in `gsheets.properties` as `metadata-sheet-id`.

4. First row should be header containing columns names as `table_name, sheet_id, owner, notes` in the same order [here](https://docs.google.com/spreadsheets/d/1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM/edit#gid=0)

## Querying a google sheet in presto

1. Open the sheet that you would like to access in presto.

2. Grant View Access on the sheet to service account user.

3. Specify a Table Name in the metadata sheet mapping (Column A). 

4. Paste the sheet ID in the same row in the mapping spreadsheet (Column B).

5. To refer to a specific sheet-tab, add the "tab name" after a "#" sign.

6. Table is ready for querying, run `SELECT * from gsheets.default.table_name`. 

## Notes
Google Sheet API has rate limits : 1. `unlimited for day`, 2. `100 calls per 100 seconds` for each Read and Write. 
You can choose higher cache duration to avoid hitting rate limits. 
Also running queries on `gsheets.information_schema.columns` may lead to hitting API rate limits as all 
table's data may not be cached at that time.
