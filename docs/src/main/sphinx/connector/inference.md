# Inference connector

The Inference connector sends rows of a table to a machine learning model and
returns the model predictions as a table. The model must be served by a model
server that implements the REST API of the [Open Inference Protocol
(V2)](https://github.com/kserve/open-inference-protocol), such as [NVIDIA Triton
Inference Server](https://github.com/triton-inference-server/server),
[OpenVINO Model Server](https://github.com/openvinotoolkit/model_server), or
[MLServer](https://github.com/SeldonIO/MLServer).

## Requirements

To use the connector, you need:

- A model server that implements the Open Inference Protocol (V2) REST API,
  including the model metadata endpoint `GET /v2/models/{model_name}`.
- Models that meet the [model requirements](inference-model-requirements).
- Network access from the Trino coordinator and workers to the model server.
  The connector sends the values of the input rows to the model server, so
  use a server inside your network, and use HTTPS for the endpoint when the
  connection is not private.

(inference-model-requirements)=
### Model requirements

The connector reads the inputs and outputs of a model from the model metadata,
and can only use a model when it supports all inputs and outputs of the model,
including outputs that a query does not use. A model must meet the following
requirements:

- The metadata of the model declares the name, data type, and shape of every
  input and output.
- The data type of every input and output is one of the data types in the
  [type mapping](inference-type-mapping).
- The first dimension of every shape is `-1`, for a variable number of rows. A
  model that accepts only a fixed number of rows, such as a model with the input
  shape `[1, 4]`, is not supported.
- Every shape has one or two dimensions: `[-1]` or `[-1, 1]` for one value per
  row, `[-1, n]` for `n` values per row, or `[-1, -1]` for any number of values
  per row. With `[-1, -1]`, all rows that the connector sends in one request
  must have the same number of values.
- The model accepts every input as a separate tensor, and up to
  `inference.max-batch-size` rows in one request.
- The model server returns every output that the metadata declares, with the
  declared name, data type, and shape, and with a result for every row of a
  request. The connector requests the outputs by these names.

Use the [describe_model](inference-describe-model) function to show the
metadata of a model. When a model does not meet the requirements, queries fail
with an error that describes the problem. To fix it, change the model, or the
configuration of the model on the model server, such as the model settings in
MLServer or the model configuration in Triton.

## Configuration

Create a catalog properties file that specifies the Inference connector by
setting the `connector.name` to `inference`.

For example, to use a model server as the `example` catalog, create the file
`etc/catalog/example.properties`:

```text
connector.name=inference
inference.endpoint=http://model-server.example.com:8000
```

The following table contains a list of all available configuration properties.

:::{list-table} Inference configuration properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `inference.endpoint`
  - Base URI of the model server that Trino cluster nodes use to access the
    Open Inference Protocol API. The URI can include a path, and the connector
    appends the paths of the API to it, such as `/v2/models/{model_name}`.
* - `inference.max-batch-size`
  - Maximum number of rows sent to the model server in a single inference
    request. The value must not exceed the batch size limit of the model on the
    model server, such as `max_batch_size` in the model configuration of
    Triton. Defaults to `1000`.
* - `inference.max-retries`
  - Maximum number of times a request to the model server is retried after a
    connection failure, a timeout, or the HTTP status `429`, `502`, `503`, or
    `504`. The delay between retries grows from 500 milliseconds to 10 seconds.
    Defaults to `3`.
* - `inference.authentication.token`
  - Token that the connector sends to the model server. See
    [](inference-authentication).
* - `inference.authentication.token-file`
  - Path to a file with the token that the connector sends to the model server.
    The connector reads the file for every request, so the token can be rotated
    without restarting Trino. Cannot be combined with
    `inference.authentication.token`.
* - `inference.authentication.header-name`
  - Name of the HTTP header with the token. Defaults to `Authorization`.
:::

The connector uses the Airlift HTTP client, configured with properties that use
the `inference.http-client` prefix. For example, set
`inference.http-client.request-timeout` to change the request timeout, and
`inference.http-client.trust-store-path` to trust the certificate of a model
server that uses HTTPS.

(inference-authentication)=
### Authentication

The Open Inference Protocol does not define authentication. Model servers, the
proxies in front of them, and managed inference endpoints check a token in an
HTTP header instead. With the default header name `Authorization`, the
connector sends the token as a bearer token, `Authorization: Bearer <token>`.
With any other header name, the connector sends the token as the value of the
header.

The following catalog uses a Kubernetes service account token for a model that
is protected by token authentication, for example in Red Hat OpenShift AI or
behind `kube-rbac-proxy`:

```text
connector.name=inference
inference.endpoint=https://fraud-model.example.com
inference.authentication.token-file=/var/run/secrets/kubernetes.io/serviceaccount/token
```

The following catalog sends the header that the restricted API feature of NVIDIA
Triton Inference Server requires, for a server started with
`--http-restricted-api=inference,metadata:infer-key=<token>`:

```text
connector.name=inference
inference.endpoint=http://triton.example.com:8000
inference.authentication.header-name=infer-key
inference.authentication.token=<token>
```

The connector fails queries with an authentication error when the model server
returns the HTTP status `401` or `403`.

(inference-models)=
### Models and versions

A catalog connects to one model server, and provides access to all models that
the server serves. Queries select a model with the `model_name` argument of the
[predict](inference-predict) function, and optionally a version with the
`model_version` argument. You do not need a catalog for each model or each model
version. Create a catalog for each model server, for example when different
teams operate separate model servers.

When a new version of a model is deployed to the model server, queries that do
not specify `model_version` use the version that the server chooses, usually
the latest version. Specify `model_version` to keep using a version, or to
compare versions.

(inference-monitoring)=
### Monitoring

The connector exports the following statistics with [JMX](/connector/jmx), in
the MBean `trino.plugin.inference:type=OpenInferenceClient,name=<catalog>` of
each Trino node:

* `MetadataRequestTime`: Duration of the requests for model metadata.
* `InferenceRequestTime`: Duration of the inference requests, including
  retries.
* `InferenceRows`: Number of rows sent to the model server.
* `Retries`: Number of retried requests.
* `FailedRequests`: Number of requests that failed after all retries.

(inference-type-mapping)=
## Type mapping

The connector maps each tensor data type of the Open Inference Protocol to a
Trino type. A model input requires a column of the mapped type. Use
[](/functions/conversion) to cast columns of other types.

:::{list-table} Open Inference Protocol to Trino type mapping
:widths: 40, 60
:header-rows: 1

* - Tensor data type
  - Trino type
* - `BOOL`
  - `BOOLEAN`
* - `INT8`
  - `TINYINT`
* - `INT16`, `UINT8`
  - `SMALLINT`
* - `INT32`, `UINT16`
  - `INTEGER`
* - `INT64`, `UINT32`
  - `BIGINT`
* - `FP16`, `FP32`
  - `REAL`
* - `FP64`
  - `DOUBLE`
* - `BYTES`
  - `VARCHAR`
:::

No other types are supported. `NaN` and infinite `REAL` and `DOUBLE` values
cannot be sent to the model server, and fail the query. The connector exchanges
tensor data with the model server as JSON, and some model servers do not
support `FP16` tensors in JSON.

The first dimension of a tensor shape is the number of rows. A tensor with shape
`[-1]` or `[-1, 1]` has one value per row, and maps to a column of the type in
the table. A tensor with shape `[-1, n]` has `n` values per row, and a tensor
with shape `[-1, -1]` has any number of values per row. Both map to an
`ARRAY` of the type in the table. For example, an ONNX model with an `FP32`
input of shape `[-1, 4]` reads an `ARRAY(REAL)` column with four elements in
each row. Tensors with more than two dimensions are not supported.

(inference-sql-support)=
## SQL support

The connector does not provide access to any schemas or tables. Use the
[predict](inference-predict) table function to score rows with a model, and the
[describe_model](inference-describe-model) table function to show the inputs
and outputs of a model.

(inference-table-functions)=
### Table functions

The connector provides the following [table functions](/functions/table).

(inference-predict)=
#### `predict(input, model_name, model_version) -> table`

The `predict` function sends the rows of the `input` table to the model
`model_name` and returns a table with one row for each input row. The table
function is available in the `system` schema of the catalog that uses the
Inference connector. It accepts the following arguments:

* `input`: The table with the rows to score. The connector reads each model
  input from the column with the same name, matched case-insensitively. Use a
  query to select the rows to score, rename columns, cast them to the expected
  types, or combine several columns into one array input.
* `model_name`: A `varchar` with the name of the model on the model server.
* `model_version`: An optional `varchar` with the version of the model. If
  omitted, the model server chooses the version.

The returned table contains one column for each model output, followed by all
columns of the `input` table. The connector sends the rows in batches of at most
`inference.max-batch-size` rows. Rows with a `NULL` value in any model input,
including a `NULL` array element, are not sent to the model, and get `NULL`
values in all output columns. Output columns use the output names from the
model metadata. Use a quoted identifier for names that are not valid SQL
identifiers, for example `"output-0"`.

The following query scores the orders of the previous day in the
`hive.sales.orders` table with the model `fraud`. The model has one input
`features` with shape `[-1, 3]`, and the outputs `label` and `probabilities`.
The filter on `order_date` is part of the input query, so only the matching
rows are read and sent to the model:

```sql
SELECT order_id, label, probabilities[2] AS fraud_probability
FROM
  TABLE(
    example.system.predict(
      input => TABLE(
        SELECT
          order_id,
          ARRAY[
            CAST(amount AS real),
            CAST(item_count AS real),
            IF(is_foreign, REAL '1', REAL '0')] AS features
        FROM hive.sales.orders
        WHERE order_date = current_date - INTERVAL '1' DAY),
      model_name => 'fraud'
    )
  );
```

To score a large table once, store the result with
[](/sql/create-table-as) or [](/sql/insert), so that later queries do not send
the rows to the model again.

(inference-describe-model)=
#### `describe_model(model_name, model_version) -> table`

The `describe_model` function returns one row for each input and output of the
model `model_name`, from the metadata of the model server. Use it to find the
columns that the input table of the [predict](inference-predict) function
requires. The function is available in the `system` schema of the catalog, and
accepts the same `model_name` and `model_version` arguments as `predict`.

The returned table has the following columns:

* `kind`: `input` or `output`.
* `name`: The name of the tensor, which is the column name in `predict`.
* `data_type`: The tensor data type, such as `FP32`.
* `shape`: The tensor shape as an `ARRAY(BIGINT)`.
* `trino_type`: The type of the column in `predict`, or `NULL` when the
  connector does not support the data type or shape of the tensor. The
  `predict` function cannot use a model with such a tensor, see
  [model requirements](inference-model-requirements).

```sql
SELECT *
FROM TABLE(example.system.describe_model(model_name => 'fraud'));
```

```text
  kind  |     name      | data_type |  shape  | trino_type
--------+---------------+-----------+---------+-------------
 input  | features      | FP32      | [-1, 3] | array(real)
 output | label         | INT64     | [-1]    | bigint
 output | probabilities | FP32      | [-1, 2] | array(real)
```

## Examples

The following examples use the model `churn` with the inputs `age` and
`monthly_spend`, and the output `churn_probability`.

The following query stores the predictions for all customers in a new table,
together with the model version and the time of the prediction:

```sql
CREATE TABLE hive.crm.churn_scores AS
SELECT
  customer_id,
  churn_probability,
  '3' AS model_version,
  current_timestamp AS scored_at
FROM
  TABLE(
    example.system.predict(
      input => TABLE(
        SELECT customer_id, age, monthly_spend
        FROM hive.crm.customers),
      model_name => 'churn',
      model_version => '3'
    )
  );
```

The following query adds predictions only for customers without a prediction:

```sql
INSERT INTO hive.crm.churn_scores
SELECT customer_id, churn_probability, '3', current_timestamp
FROM
  TABLE(
    example.system.predict(
      input => TABLE(
        SELECT customer_id, age, monthly_spend
        FROM hive.crm.customers c
        WHERE NOT EXISTS (
          SELECT 1 FROM hive.crm.churn_scores s
          WHERE s.customer_id = c.customer_id)),
      model_name => 'churn',
      model_version => '3'
    )
  );
```

The following query compares two versions of the model, and returns the
customers with the largest differences between the predictions. A table alias
of a table function only applies to the output columns of the model, so the
query uses subqueries to refer to the `customer_id` input column with the
aliases `v2` and `v3`:

```sql
SELECT
  v2.customer_id,
  v2.churn_probability AS version_2,
  v3.churn_probability AS version_3
FROM
  (SELECT * FROM TABLE(
    example.system.predict(
      input => TABLE(SELECT customer_id, age, monthly_spend FROM hive.crm.customers),
      model_name => 'churn',
      model_version => '2'))) v2
  JOIN
  (SELECT * FROM TABLE(
    example.system.predict(
      input => TABLE(SELECT customer_id, age, monthly_spend FROM hive.crm.customers),
      model_name => 'churn',
      model_version => '3'))) v3
  ON v2.customer_id = v3.customer_id
ORDER BY abs(v2.churn_probability - v3.churn_probability) DESC
LIMIT 10;
```
