# trino-open-policy-agent

This plugin enables Trino to use Open Policy Agent (OPA) as an authorization engine.

For more information on OPA, please refer to the Open Policy Agent [documentation](https://www.openpolicyagent.org/).

## Configuration

You will need to configure Trino to use the OPA plugin as its access control engine, then configure the
plugin to contact your OPA endpoint.

`config.properties` - **enabling the plugin**:

Make sure to enable the plugin by configuring Trino to pull in the relevant config file for the OPA
authorizer, e.g.:

```properties
access-control.config-files=/etc/trino/access-control-file-based.properties,/etc/trino/access-control-opa.properties
```

`access-control-opa.properties` - **configuring the plugin**:

Set the access control name to `opa` and specify the policy URI, for example:

```properties
access-control.name=opa
opa.policy.uri=https://your-opa-endpoint/v1/data/allow
```

If you also want to enable the _batch_ mode (see [Batch mode](#batch-mode)), you must additionally set up an
`opa.policy.batched-uri` configuration entry.

> Batch mode is _not_ a replacement for the "main" URI. The batch mode is _only_
> used for certain authorization queries where batching is applicable. Even when using
> `opa.policy.batched-uri`, you _must_ still provide an `opa.policy.uri`

For instance:

```properties
access-control.name=opa
opa.policy.uri=https://your-opa-endpoint/v1/data/allow
opa.policy.batched-uri=https://your-opa-endpoint/v1/data/batch
```

## OPA queries

The plugin will contact OPA for each authorization request as defined on the SPI.

OPA must return a response containing a boolean `allow` field, which will determine whether the operation
is permitted or not.

The plugin will pass as much context as possible within the OPA request. A simple way of checking
what data is passed in from Trino is to run OPA locally in verbose mode.

### Query structure

A query will contain a `context` and an `action` as its top level fields.

#### Query context:

This determines _who_ is performing the operations, and reflects the `SystemSecurityContext` class in Trino.

#### Query action:

This determines _what_ action is being performed and upon what resources, the top level fields are as follows:

- `operation` (string): operation being performed
- `resource` (object, nullable): information about the object being operated upon
- `targetResource` (object, nullable): information about the _new object_ being created, if applicable
- `grantee` (object, nullable): grantee of a grant operation
- `grantor` (object, nullable): grantor in a grant operation

#### Examples

Accessing a table will result in a query like the one below:

```json
{
  "context": {
    "identity": {
      "user": "foo",
      "groups": ["some-group"]
    }
  },
  "action": {
    "operation": "SelectFromColumns",
    "resource": {
      "table": {
        "catalogName": "my_catalog",
        "schemaName": "my_schema",
        "tableName": "my_table",
        "columns": [
          "column1",
          "column2",
          "column3"
        ]
      }
    }
  }
}
```

`targetResource` is used in cases where a new resource, distinct from the one in `resource` is being created. For instance,
when renaming a table.

```json
{
  "context": {
    "identity": {
      "user": "foo",
      "groups": ["some-group"]
    }
  },
  "action": {
    "operation": "RenameTable",
    "resource": {
      "table": {
        "catalogName": "my_catalog",
        "schemaName": "my_schema",
        "tableName": "my_table"
      }
    },
    "targetResource": {
      "table": {
        "catalogName": "my_catalog",
        "schemaName": "my_schema",
        "tableName": "new_table_name"
      }
    }
  }
}
```


## Batch mode

A very powerful feature provided by OPA is its ability to respond to authorization queries with
more complex answers than a `true`/`false` boolean value.

Many features in Trino require _filtering_ to be performed to determine, given a list of resources,
(e.g. tables, queries, views, etc...) which of those a user should be entitled to see/interact with.

If `opa.policy.batched-uri` is _not_ configured, the plugin will send one request to OPA _per item_ being
filtered, then use the responses from OPA to construct a filtered list containing only those items for which
a `true` response was returned.

Configuring `opa.policy.batched-uri` will allow the plugin to send a request to that _batch_ endpoint instead,
with a **list** of the resources being filtered under `action.filterResources` (as opposed to `action.resource`).

> The other fields in the request are identical to the non-batch endpoint.

An OPA policy supporting batch operations should return a (potentially empty) list containing the _indices_
of the items for which authorization is granted (if any). Returning a `null` value instead of a list
is equivalent to returning an empty list.

> We may want to reconsider the choice of using _indices_ in the response as opposed to returning a list
> containing copies of elements from the `filterResources` field in the request for which access should
> be granted. Indices were chosen over copying elements as it made validation in the plugin easier,
> and from the few examples we tried, it also made certain policies a bit simpler. Any feedback is appreciated!

An interesting side effect of this is that we can add batching support for policies that didn't originally
have it quite easily. Consider the following rego:

```rego
package foo

# ... rest of the policy ...
# this assumes the non-batch response field is called "allow"
batch contains i {
    some i
    raw_resource := input.action.filterResources[i]
    allow with input.action.resource as raw_resource
}

# Corner case: filtering columns is done with a single table item, and many columns inside
# We cannot use our normal logic in other parts of the policy as they are based on sets
# and we need to retain order
batch contains i {
    some i
    input.action.operation == "FilterColumns"
    count(input.action.filterResources) == 1
    raw_resource := input.action.filterResources[0]
    count(raw_resource["table"]["columns"]) > 0
    new_resources := [
        object.union(raw_resource, {"table": {"column": col}})
        | col := raw_resource["table"]["columns"][_]
    ]
    allow with input.action.resource as new_resources[i]
}
```
