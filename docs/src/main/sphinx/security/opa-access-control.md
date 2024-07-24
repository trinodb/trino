# Open Policy Agent access control

The Open Policy Agent access control plugin enables the use of [Open Policy
Agent (OPA)](https://www.openpolicyagent.org/) as authorization engine for
fine-grained access control to catalogs, schemas, tables, and more in Trino.
Policies are defined in OPA, and Trino checks access control privileges in OPA.

## Requirements

* A running [OPA deployment](https://www.openpolicyagent.org/docs/latest/#running-opa)
* Network connectivity from the Trino cluster to the OPA server

With the requirements fulfilled, you can proceed to set up Trino and OPA with
your desired access control configuration.

## Trino configuration

To use only OPA for access control, create the file `etc/access-control.properties`
with the following minimal configuration:

```properties
access-control.name=opa
opa.policy.uri=https://your-opa-endpoint/v1/data/allow
```

To combine OPA access control with file-based or other access control systems,
configure multiple access control configuration file paths in
`etc/config.properties`:

```properties
access-control.config-files=etc/trino/file-based.properties,etc/trino/opa.properties
```

Order the configuration files list in the desired order of the different systems
for overall access control. Configure each access-control system in the
specified files.

The following table lists the configuration properties for the OPA access control:

:::{list-table} OPA access control configuration properties
:widths: 40, 60
:header-rows: 1

* - Name
  - Description
* - `opa.policy.uri`
  - The **required** URI for the OPA endpoint, for example,
    `https://opa.example.com/v1/data/allow`.
* - `opa.policy.batched-uri`
  - The **optional** URI for activating batch mode for certain authorization
    queries where batching is applicable, for example
    `https://opa.example.com/v1/data/batch`. Batch mode is described
    [](opa-batch-mode).
* - `opa.log-requests`
  - Configure if request details, including URI, headers and the entire body, are
    logged prior to sending them to OPA. Defaults to `false`.
* - `opa.log-responses`
  - Configure if OPA response details, including URI, status code, headers and
    the entire body, are logged. Defaults to `false`.
* - `opa.allow-permission-management`
  - Configure if permission management operations are allowed. Find more details in
    [](opa-permission-management). Defaults to `false`.
* - `opa.http-client.*`
  - Optional HTTP client configurations for the connection from Trino to OPA,
    for example `opa.http-client.http-proxy` for configuring the HTTP proxy.
    Find more details in [](/admin/properties-http-client).
:::

### Logging

When request or response logging is enabled, details are logged at the `DEBUG`
level under the `io.trino.plugin.opa.OpaHttpClient` logger. The Trino logging
configuration must be updated to include this class, to ensure log entries are
created.

Note that enabling these options produces very large amounts of log data.

(opa-permission-management)=
### Permission management

The following operations are allowed or denied based on the setting of
`opa.allow-permission-management` If set to `true`, these operations are
allowed. If set to `false`, they are denied. In both cases, no request is sent
to OPA.

- `GrantSchemaPrivilege`
- `DenySchemaPrivilege`
- `RevokeSchemaPrivilege`
- `GrantTablePrivilege`
- `DenyTablePrivilege`
- `RevokeTablePrivilege`
- `CreateRole`
- `DropRole`
- `GrantRoles`
- `RevokeRoles`

The setting defaults to `false` due to the complexity and potential unexpected
consequences of having SQL-style grants and roles together with OPA.

You must enable permission management if another custom security system in Trino
is capable of grant management and used together with OPA access control.

Additionally, users are always allowed to show information about roles (`SHOW
ROLES`), regardless of this setting. The following operations are _always_
allowed:

- `ShowRoles`
- `ShowCurrentRoles`
- `ShowRoleGrants`

## OPA configuration

The OPA access control in Trino contacts OPA for each query and issues an
authorization request. OPA must return a response containing a boolean `allow`
field, which determines whether the operation is permitted or not.

Policies in OPA are defined with the purpose built policy language Rego. Find
more information in the [detailed
documentation](https://www.openpolicyagent.org/docs/latest/policy-language/).
After the initial installation and configuration in Trino, these policies are
the main configuration aspect for your access control setup.

A query from the OPA access control in Trino to OPA contains a `context` and an
`action` as its top level fields.

The `context` object contains all other contextual information about the query:

- `identity`: The identity of the user performing the operation, containing the
  following two fields:
  - `user`: username
  - `groups`: list of groups this user belongs to
- `softwareStack`: Information about the software stack issuing the request to
  OPA. The following information is included:
  - `trinoVersion`: Version of Trino used

The `action` object contains information about what action is performed on what
resources. The following fields are provided:

- `operation`: the performed operation, for example `SelectFromColumns`.
- `resource`: information about the accessed objects
- `targetResource`: information about any newly created object, if applicable
- `grantee`: grantee of a grant operation.

Fields that are not applicable for a specific operation are set to null.
Examples are an empty  `targetResource` if not modifying a table or schema or
catalog is modified, or an empty `grantee` if not granting permissions is set.
Any null field is omitted altogether from the `action` object.

### Example requests to OPA

Accessing a table results in a query similar to the following example:

```json
{
  "context": {
    "identity": {
      "user": "foo",
      "groups": ["some-group"]
    },
    "softwareStack": {
      "trinoVersion": "434"
    }
  },
  "action": {
    "operation": "SelectFromColumns",
    "resource": {
      "table": {
        "catalogName": "example_catalog",
        "schemaName": "example_schema",
        "tableName": "example_table",
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

The `targetResource` is used in cases where a new resource, distinct from the one in
`resource` is created. For example, when renaming a table.

```json
{
  "context": {
    "identity": {
      "user": "foo",
      "groups": ["some-group"]
    },
    "softwareStack": {
      "trinoVersion": "434"
    }
  },
  "action": {
    "operation": "RenameTable",
    "resource": {
      "table": {
        "catalogName": "example_catalog",
        "schemaName": "example_schema",
        "tableName": "example_table"
      }
    },
    "targetResource": {
      "table": {
        "catalogName": "example_catalog",
        "schemaName": "example_schema",
        "tableName": "new_table_name"
      }
    }
  }
}
```

(opa-batch-mode)=
## Batch mode

A very powerful feature provided by OPA is its ability to respond to
authorization queries with more complex answers than a `true` or `false` boolean
value.

Many features in Trino require filtering to determine to which resources a user
is granted access. These resources are catalogs, schema, queries, views, and
others objects.

If `opa.policy.batched-uri` is not configured, Trino sends one request to OPA
for each object, and then creates a filtered list of permitted objects.

Configuring `opa.policy.batched-uri` allows Trino to send a request to
the batch endpoint, with a list of resources in one request using the
under `action.filterResources` node.

All other fields in the request are identical to the non-batch endpoint.

An OPA policy supporting batch operations must return a list containing the
_indices_ of the items for which authorization is granted. Returning a `null`
value or an empty list is equivalent and denies any access.

You can add batching support for policies that do not support it:

```text
package foo

import future.keywords.contains

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
        object.union(raw_resource, {"table": {"column": column_name}})
        | column_name := raw_resource["table"]["columns"][_]
    ]
    allow with input.action.resource as new_resources[i]
}
```
