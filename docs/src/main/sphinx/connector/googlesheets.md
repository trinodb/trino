# Google Sheets connector

```{raw} html
<img src="../_static/img/google-sheets.png" class="connector-logo">
```

The Google Sheets connector allows reading and writing [Google Sheets](https://www.google.com/sheets/about/) spreadsheets as tables in Trino.

## Configuration

Create `etc/catalog/example.properties` to mount the Google Sheets connector
as the `example` catalog, with the following contents:

```text
connector.name=gsheets
gsheets.credentials-path=/path/to/google-sheets-credentials.json
gsheets.metadata-sheet-id=exampleId
```

## Configuration properties

The following configuration properties are available:

| Property name                 | Description                                                      |
| ----------------------------- | ---------------------------------------------------------------- |
| `gsheets.credentials-path`    | Path to the Google API JSON key file                             |
| `gsheets.credentials-key`     | The base64 encoded credentials key                               |
| `gsheets.metadata-sheet-id`   | Sheet ID of the spreadsheet, that contains the table mapping     |
| `gsheets.max-data-cache-size` | Maximum number of spreadsheets to cache, defaults to `1000`      |
| `gsheets.data-cache-ttl`      | How long to cache spreadsheet data or metadata, defaults to `5m` |
| `gsheets.connection-timeout`  | Timeout when connection to Google Sheets API, defaults to `20s`  |
| `gsheets.read-timeout`        | Timeout when reading from Google Sheets API, defaults to `20s`   |
| `gsheets.write-timeout`       | Timeout when writing to Google Sheets API, defaults to `20s`     |

## Credentials

The connector requires credentials in order to access the Google Sheets API.

1. Open the [Google Sheets API](https://console.developers.google.com/apis/library/sheets.googleapis.com)
   page and click the *Enable* button. This takes you to the API manager page.
2. Select a project using the drop down menu at the top of the page.
   Create a new project, if you do not already have one.
3. Choose *Credentials* in the left panel.
4. Click *Manage service accounts*, then create a service account for the connector.
   On the *Create key* step, create and download a key in JSON format.

The key file needs to be available on the Trino coordinator and workers.
Set the `gsheets.credentials-path` configuration property to point to this file.
The exact name of the file does not matter -- it can be named anything.

Alternatively, set the `gsheets.credentials-key` configuration property.
It should contain the contents of the JSON file, encoded using base64.

## Metadata sheet

The metadata sheet is used to map table names to sheet IDs.
Create a new metadata sheet. The first row must be a header row
containing the following columns in this order:

- Table Name
- Sheet ID
- Owner (optional)
- Notes (optional)

See this [example sheet](https://docs.google.com/spreadsheets/d/1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM)
as a reference.

The metadata sheet must be shared with the service account user,
the one for which the key credentials file was created. Click the *Share*
button to share the sheet with the email address of the service account.

Set the `gsheets.metadata-sheet-id` configuration property to the ID of this sheet.

## Querying sheets

The service account user must have access to the sheet in order for Trino
to query it. Click the *Share* button to share the sheet with the email
address of the service account.

The sheet needs to be mapped to a Trino table name. Specify a table name
(column A) and the sheet ID (column B) in the metadata sheet. To refer
to a specific range in the sheet, add the range after the sheet ID, separated
with `#`. If a range is not provided, the connector loads only 10,000 rows by default from
the first tab in the sheet.

The first row of the provided sheet range is used as the header and will determine the column
names of the Trino table.
For more details on sheet range syntax see the [google sheets docs](https://developers.google.com/sheets/api/guides/concepts).

## Writing to sheets

The same way sheets can be queried, they can also be written by appending data to existing sheets.
In this case the service account user must also have **Editor** permissions on the sheet.

After data is written to a table, the table contents are removed from the cache
described in [API usage limits](gsheets-api-usage). If the table is accessed
immediately after the write, querying the Google Sheets API may not reflect the
change yet. In that case the old version of the table is read and cached for the
configured amount of time, and it might take some time for the written changes
to propagate properly.

Keep in mind that the Google Sheets API has [usage limits](https://developers.google.com/sheets/api/limits), that limit the speed of inserting data.
If you run into timeouts you can increase timeout times to avoid `503: The service is currently unavailable` errors.

(gsheets-api-usage)=
## API usage limits

The Google Sheets API has [usage limits](https://developers.google.com/sheets/api/limits),
that may impact the usage of this connector. Increasing the cache duration and/or size
may prevent the limit from being reached. Running queries on the `information_schema.columns`
table without a schema and table name filter may lead to hitting the limit, as this requires
fetching the sheet data for every table, unless it is already cached.

## Type mapping

Because Trino and Google Sheets each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading data.

### Google Sheets type to Trino type mapping

The connector maps Google Sheets types to the corresponding Trino types
following this table:

```{eval-rst}
.. list-table:: Google Sheets type to Trino type mapping
  :widths: 30, 20
  :header-rows: 1

  * - Google Sheets type
    - Trino type
  * - ``TEXT``
    - ``VARCHAR``
```

No other types are supported.

(google-sheets-sql-support)=

## SQL support

In addition to the {ref}`globally available <sql-globally-available>` and {ref}`read operation <sql-read-operations>` statements,
this connector supports the following features:

- {doc}`/sql/insert`

## Table functions

The connector provides specific {doc}`/functions/table` to access Google Sheets.

(google-sheets-sheet-function)=

### `sheet(id, range) -> table`

The `sheet` function allows you to query a Google Sheet directly without
specifying it as a named table in the metadata sheet.

For example, for a catalog named 'example':

```
SELECT *
FROM
  TABLE(example.system.sheet(
      id => 'googleSheetIdHere'));
```

A sheet range or named range can be provided as an optional `range` argument.
The default sheet range is `$1:$10000` if one is not provided:

```
SELECT *
FROM
  TABLE(example.system.sheet(
      id => 'googleSheetIdHere',
      range => 'TabName!A1:B4'));
```
