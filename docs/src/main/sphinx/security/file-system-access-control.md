# File-based access control

To secure access to data in your cluster, you can implement file-based access
control where access to data and operations is defined by rules declared in
manually-configured JSON files.

There are two types of file-based access control:

- **System-level access control** uses the access control plugin with a single
  JSON file that specifies authorization rules for the whole cluster.
- **Catalog-level access control** uses individual JSON files for each catalog
  for granular control over the data in that catalog, including column-level
  authorization.

(system-file-based-access-control)=

## System-level access control files

The access control plugin allows you to specify authorization rules for the
cluster in a single JSON file.

### Configuration

:::{warning}
Access to all functions including {doc}`table functions </functions/table>` is allowed by default.
To mitigate unwanted access, you must add a `function`
{ref}`rule <system-file-function-rules>` to deny the `TABLE` function type.
:::

To use the access control plugin, add an `etc/access-control.properties` file
containing two required properties: `access-control.name`, which must be set
to `file`, and `security.config-file`, which must be set to the location
of the config file. The configuration file location can either point to the local
disc or to a http endpoint. For example, if a config file named `rules.json` resides
in `etc`, add an `etc/access-control.properties` with the following
contents:

```text
access-control.name=file
security.config-file=etc/rules.json
```

If the config should be loaded via the http endpoint `http://trino-test/config` and
is wrapped into a JSON object and available via the `data` key `etc/access-control.properties`
should look like this:

```text
access-control.name=file
security.config-file=http://trino-test/config
security.json-pointer=/data
```

The config file is specified in JSON format. It contains rules that define which
users have access to which resources. The rules are read from top to bottom and
the first matching rule is applied. If no rule matches, access is denied. A JSON
pointer (RFC 6901) can be specified using the `security.json-pointer` property
to specify a nested object inside the JSON content containing the rules. Per default,
the file is assumed to contain a single object defining the rules rendering
the specification of `security.json-pointer` unnecessary in that case.

### Refresh

By default, when a change is made to the JSON rules file, Trino must be
restarted to load the changes. There is an optional property to refresh the
properties without requiring a Trino restart. The refresh period is specified in
the `etc/access-control.properties`:

```text
security.refresh-period=1s
```

### Catalog, schema, and table access

Access to catalogs, schemas, tables, and views is controlled by the catalog,
schema, and table rules. The catalog rules are coarse-grained rules used to
restrict all access or write access to catalogs. They do not explicitly grant
any specific schema or table permissions. The table and schema rules are used to
specify who can create, drop, alter, select, insert, delete, etc. for schemas
and tables.

:::{note}
These rules do not apply to system-defined tables in the
`information_schema` schema.
:::

For each rule set, permission is based on the first matching rule read from top
to bottom.  If no rule matches, access is denied. If no rules are provided at
all, then access is granted.

The following table summarizes the permissions required for each SQL command:

| SQL command                        | Catalog   | Schema  | Table                | Note                                                              |
| ---------------------------------- | --------- | ------- | -------------------- | ----------------------------------------------------------------- |
| SHOW CATALOGS                      |           |         |                      | Always allowed                                                    |
| SHOW SCHEMAS                       | read-only | any\*   | any\*                | Allowed if catalog is {ref}`visible<system-file-auth-visibility>` |
| SHOW TABLES                        | read-only | any\*   | any\*                | Allowed if schema {ref}`visible<system-file-auth-visibility>`     |
| CREATE SCHEMA                      | read-only | owner   |                      |                                                                   |
| DROP SCHEMA                        | all       | owner   |                      |                                                                   |
| SHOW CREATE SCHEMA                 | all       | owner   |                      |                                                                   |
| ALTER SCHEMA ... RENAME TO         | all       | owner\* |                      | Ownership is required on both old and new schemas                 |
| ALTER SCHEMA ... SET AUTHORIZATION | all       | owner   |                      |                                                                   |
| CREATE TABLE                       | all       |         | owner                |                                                                   |
| DROP TABLE                         | all       |         | owner                |                                                                   |
| ALTER TABLE ... RENAME TO          | all       |         | owner\*              | Ownership is required on both old and new tables                  |
| ALTER TABLE ... SET PROPERTIES     | all       |         | owner                |                                                                   |
| CREATE VIEW                        | all       |         | owner                |                                                                   |
| DROP VIEW                          | all       |         | owner                |                                                                   |
| ALTER VIEW ... RENAME TO           | all       |         | owner\*              | Ownership is required on both old and new views                   |
| REFRESH MATERIALIZED VIEW          | all       |         | update               |                                                                   |
| COMMENT ON TABLE                   | all       |         | owner                |                                                                   |
| COMMENT ON COLUMN                  | all       |         | owner                |                                                                   |
| ALTER TABLE ... ADD COLUMN         | all       |         | owner                |                                                                   |
| ALTER TABLE ... DROP COLUMN        | all       |         | owner                |                                                                   |
| ALTER TABLE ... RENAME COLUMN      | all       |         | owner                |                                                                   |
| SHOW COLUMNS                       | all       |         | any                  |                                                                   |
| SELECT FROM table                  | read-only |         | select               |                                                                   |
| SELECT FROM view                   | read-only |         | select, grant_select |                                                                   |
| INSERT INTO                        | all       |         | insert               |                                                                   |
| DELETE FROM                        | all       |         | delete               |                                                                   |
| UPDATE                             | all       |         | update               |                                                                   |

Permissions required for executing functions:

```{eval-rst}
.. list-table::
   :widths: 30, 10, 15, 15, 30
   :header-rows: 1

   * - SQL command
     - Catalog
     - Function permission
     - Function kind
     - Note
   * - ``SELECT function()``
     -
     - ``execute``, ``grant_execute*``
     - ``aggregate``, ``scalar``, ``window``
     - ``grant_execute`` is required when function is executed with view owner privileges.
   * - ``SELECT FROM TABLE(table_function())``
     - ``all``
     - ``execute``, ``grant_execute*``
     - ``table``
     - ``grant_execute`` is required when :doc:`table function </functions/table>` is executed with view owner privileges.
```

(system-file-auth-visibility)=

#### Visibility

For a catalog, schema, or table to be visible in a `SHOW` command, the user
must have at least one permission on the item or any nested item. The nested
items do not need to already exist as any potential permission makes the item
visible. Specifically:

- `catalog`: Visible if user is the owner of any nested schema, has
  permissions on any nested table or {doc}`table function </functions/table>`, or has permissions to
  set session properties in the catalog.
- `schema`: Visible if the user is the owner of the schema, or has permissions
  on any nested table or {doc}`table function </functions/table>`.
- `table`: Visible if the user has any permissions on the table.

#### Catalog rules

Each catalog rule is composed of the following fields:

- `user` (optional): regex to match against user name. Defaults to `.*`.
- `role` (optional): regex to match against role names. Defaults to `.*`.
- `group` (optional): regex to match against group names. Defaults to `.*`.
- `catalog` (optional): regex to match against catalog name. Defaults to
  `.*`.
- `allow` (required): string indicating whether a user has access to the
  catalog. This value can be `all`, `read-only` or `none`, and defaults to
  `none`. Setting this value to `read-only` has the same behavior as the
  `read-only` system access control plugin.

In order for a rule to apply the user name must match the regular expression
specified in `user` attribute.

For role names, a rule can be applied if at least one of the currently enabled
roles matches the `role` regular expression.

For group names, a rule can be applied if at least one group name of this user
matches the `group` regular expression.

The `all` value for `allow` means these rules do not restrict access in any
way, but the schema and table rules can restrict access.

:::{note}
By default, all users have access to the `system` catalog. You can
override this behavior by adding a rule.

Boolean `true` and `false` are also supported as legacy values for
`allow`, to support backwards compatibility.  `true` maps to `all`,
and `false` maps to `none`.
:::

For example, if you want to allow only the role `admin` to access the
`mysql` and the `system` catalog, allow users from the `finance` and
`human_resources` groups access to `postgres` catalog, allow all users to
access the `hive` catalog, and deny all other access, you can use the
following rules:

```json
{
  "catalogs": [
    {
      "role": "admin",
      "catalog": "(mysql|system)",
      "allow": "all"
    },
    {
      "group": "finance|human_resources",
      "catalog": "postgres",
      "allow": true
    },
    {
      "catalog": "hive",
      "allow": "all"
    },
    {
      "user": "alice",
      "catalog": "postgresql",
      "allow": "read-only"
    },
    {
      "catalog": "system",
      "allow": "none"
    }
  ]
}
```

For group-based rules to match, users need to be assigned to groups by a
{doc}`/develop/group-provider`.

#### Schema rules

Each schema rule is composed of the following fields:

- `user` (optional): regex to match against user name. Defaults to `.*`.
- `role` (optional): regex to match against role names. Defaults to `.*`.
- `group` (optional): regex to match against group names. Defaults to `.*`.
- `catalog` (optional): regex to match against catalog name. Defaults to
  `.*`.
- `schema` (optional): regex to match against schema name. Defaults to
  `.*`.
- `owner` (required): boolean indicating whether the user is to be considered
  an owner of the schema. Defaults to `false`.

For example, to provide ownership of all schemas to role `admin`, treat all
users as owners of the `default.default` schema and prevent user `guest`
from ownership of any schema, you can use the following rules:

```json
{
  "schemas": [
    {
      "role": "admin",
      "schema": ".*",
      "owner": true
    },
    {
      "user": "guest",
      "owner": false
    },
    {
      "catalog": "default",
      "schema": "default",
      "owner": true
    }
  ]
}
```

#### Table rules

Each table rule is composed of the following fields:

- `user` (optional): regex to match against user name. Defaults to `.*`.
- `role` (optional): regex to match against role names. Defaults to `.*`.
- `group` (optional): regex to match against group names. Defaults to `.*`.
- `catalog` (optional): regex to match against catalog name. Defaults to
  `.*`.
- `schema` (optional): regex to match against schema name. Defaults to `.*`.
- `table` (optional): regex to match against table names. Defaults to `.*`.
- `privileges` (required): zero or more of `SELECT`, `INSERT`,
  `DELETE`, `UPDATE`, `OWNERSHIP`, `GRANT_SELECT`
- `columns` (optional): list of column constraints.
- `filter` (optional): boolean filter expression for the table.
- `filter_environment` (optional): environment use during filter evaluation.

#### Column constraint

These constraints can be used to restrict access to column data.

- `name`: name of the column.
- `allow` (optional): if false, column can not be accessed.
- `mask` (optional): mask expression applied to column.
- `mask_environment` (optional): environment use during mask evaluation.

#### Filter and mask environment

- `user` (optional): username for checking permission of subqueries in mask.

:::{note}
These rules do not apply to `information_schema`.

`mask` can contain conditional expressions such as `IF` or `CASE`, which achieves conditional masking.
:::

The example below defines the following table access policy:

- Role `admin` has all privileges across all tables and schemas
- User `banned_user` has no privileges
- All users have `SELECT` privileges on `default.hr.employees`, but the
  table is filtered to only the row for the current user.
- All users have `SELECT` privileges on all tables in the `default.default`
  schema, except for the `address` column which is blocked, and `ssn` which
  is masked.

```json
{
  "tables": [
    {
      "role": "admin",
      "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"]
    },
    {
      "user": "banned_user",
      "privileges": []
    },
    {
      "catalog": "default",
      "schema": "hr",
      "table": "employee",
      "privileges": ["SELECT"],
      "filter": "user = current_user",
      "filter_environment": {
        "user": "system_user"
      }
    },
    {
      "catalog": "default",
      "schema": "default",
      "table": ".*",
      "privileges": ["SELECT"],
      "columns" : [
         {
            "name": "address",
            "allow": false
         },
         {
            "name": "SSN",
            "mask": "'XXX-XX-' + substring(credit_card, -4)",
            "mask_environment": {
              "user": "system_user"
            }
         }
      ]
    }
  ]
}
```

(system-file-function-rules)=

#### Function rules

These rules control the user's ability to execute SQL all function kinds,
such as {doc}`aggregate functions </functions/aggregate>`, scalar functions,
{doc}`table functions </functions/table>` and {doc}`window functions </functions/window>`.

Each function rule is composed of the following fields:

- `user` (optional): regular expression to match against user name.
  Defaults to `.*`.
- `role` (optional): regular expression to match against role names.
  Defaults to `.*`.
- `group` (optional): regular expression to match against group names.
  Defaults to `.*`.
- `catalog` (optional): regular expression to match against catalog name.
  Defaults to `.*`.
- `schema` (optional): regular expression to match against schema name.
  Defaults to `.*`.
- `function` (optional): regular expression to match against function names.
  Defaults to `.*`.
- `privileges` (required): zero or more of `EXECUTE`, `GRANT_EXECUTE`.
- `function_kinds` (required): one or more of `AGGREGATE`, `SCALAR`,
  `TABLES`, `WINDOW`. When a user defines a rule for `AGGREGATE`, `SCALAR`
  or `WINDOW` functions, the `catalog` and `schema` fields are disallowed
  because those functions are available globally without any catalogs involvement.

To deny all {doc}`table functions </functions/table>` from any catalog,
use the following rules:

```json
{
  "functions": [
    {
      "privileges": [
        "EXECUTE",
        "GRANT_EXECUTE"
      ],
      "function_kinds": [
        "SCALAR",
        "AGGREGATE",
        "WINDOW"
      ]
    },
    {
      "privileges": [],
      "function_kinds": [
        "TABLE"
      ]
    }
  ]
}
```

It's a good practice to limit access to `query` table function because this
table function works like a query passthrough and ignores  `tables` rules.
The following example allows the `admin` user to execute `query` table
function from any catalog:

```json
{
  "functions": [
    {
      "privileges": [
        "EXECUTE",
        "GRANT_EXECUTE"
      ],
      "function_kinds": [
        "SCALAR",
        "AGGREGATE",
        "WINDOW"
      ]
    },
    {
      "user": "admin",
      "function": "query",
      "privileges": [
        "EXECUTE"
      ],
      "function_kinds": [
        "TABLE"
      ]
    }
  ]
}
```

(verify-rules)=

#### Verify configuration

To verify the system-access control file is configured properly, set the
rules to completely block access to all users of the system:

```json
{
  "catalogs": [
    {
      "catalog": "system",
      "allow": "none"
    }
  ]
}
```

Restart your cluster to activate the rules for your cluster. With the
Trino {doc}`CLI </client/cli>` run a query to test authorization:

```text
trino> SELECT * FROM system.runtime.nodes;
Query 20200824_183358_00000_c62aw failed: Access Denied: Cannot access catalog system
```

Remove these rules and restart the Trino cluster.

(system-file-auth-session-property)=

### Session property rules

These rules control the ability of a user to set system and catalog session
properties. The user is granted or denied access, based on the first matching
rule, read from top to bottom. If no rules are specified, all users are allowed
set any session property. If no rule matches, setting the session property is
denied. System session property rules are composed of the following fields:

- `user` (optional): regex to match against user name. Defaults to `.*`.
- `role` (optional): regex to match against role names. Defaults to `.*`.
- `group` (optional): regex to match against group names. Defaults to `.*`.
- `property` (optional): regex to match against the property name. Defaults to
  `.*`.
- `allow` (required): boolean indicating if the setting the session
  property should be allowed.

The catalog session property rules have the additional field:

- `catalog` (optional): regex to match against catalog name. Defaults to
  `.*`.

The example below defines the following table access policy:

- Role `admin` can set all session property
- User `banned_user` can not set any session properties
- All users can set the `resource_overcommit` system session property, and the
  `bucket_execution_enabled` session property in the `hive` catalog.

```{literalinclude} session-property-access.json
:language: json
```

(query-rules)=

### Query rules

These rules control the ability of a user to execute, view, or kill a query. The
user is granted or denied access, based on the first matching rule read from top
to bottom. If no rules are specified, all users are allowed to execute queries,
and to view or kill queries owned by any user. If no rule matches, query
management is denied. Each rule is composed of the following fields:

- `user` (optional): regex to match against user name. Defaults to `.*`.
- `role` (optional): regex to match against role names. Defaults to `.*`.
- `group` (optional): regex to match against group names. Defaults to `.*`.
- `queryOwner` (optional): regex to match against the query owner name.
  Defaults to `.*`.
- `allow` (required): set of query permissions granted to user. Values:
  `execute`, `view`, `kill`

:::{note}
Users always have permission to view or kill their own queries.

A rule that includes `queryOwner` may not include the `execute` access mode.
Queries are only owned by a user once their execution has begun.
:::

For example, if you want to allow the role `admin` full query access, allow
the user `alice` to execute and kill queries, allow members of the group
`contractors` to view queries owned by users `alice` or `dave`, allow any
user to execute queries, and deny all other access, you can use the following
rules:

```{literalinclude} query-access.json
:language: json
```

(system-file-auth-impersonation-rules)=

### Impersonation rules

These rules control the ability of a user to impersonate another user. In
some environments it is desirable for an administrator (or managed system) to
run queries on behalf of other users. In these cases, the administrator
authenticates using their credentials, and then submits a query as a different
user. When the user context is changed, Trino verifies that the administrator
is authorized to run queries as the target user.

When these rules are present, the authorization is based on the first matching
rule, processed from top to bottom. If no rules match, the authorization is
denied. If impersonation rules are not present but the legacy principal rules
are specified, it is assumed impersonation access control is being handled by
the principal rules, so impersonation is allowed. If neither impersonation nor
principal rules are defined, impersonation is not allowed.

Each impersonation rule is composed of the following fields:

- `original_user` (optional): regex to match against the user requesting the
  impersonation. Defaults to `.*`.
- `original_role` (optional): regex to match against role names of the
  requesting impersonation. Defaults to `.*`.
- `new_user` (required): regex to match against the user to impersonate. Can
  contain references to subsequences captured during the match against
  *original_user*, and each reference is replaced by the result of evaluating
  the corresponding group respectively.
- `allow` (optional): boolean indicating if the authentication should be
  allowed. Defaults to `true`.

The impersonation rules are a bit different than the other rules: The attribute
`new_user` is required to not accidentally prevent more access than intended.
Doing so it was possible to make the attribute `allow` optional.

The following example allows the `admin` role, to impersonate any user, except
for `bob`. It also allows any user to impersonate the `test` user. It also
allows a user in the form `team_backend` to impersonate the
`team_backend_sandbox` user, but not arbitrary users:

```{literalinclude} user-impersonation.json
:language: json
```

(system-file-auth-principal-rules)=

### Principal rules

:::{warning}
Principal rules are deprecated. Instead, use {doc}`/security/user-mapping`
which specifies how a complex authentication user name is mapped to a simple
user name for Trino, and impersonation rules defined above.
:::

These rules serve to enforce a specific matching between a principal and a
specified user name. The principal is granted authorization as a user, based
on the first matching rule read from top to bottom. If no rules are specified,
no checks are performed. If no rule matches, user authorization is denied.
Each rule is composed of the following fields:

- `principal` (required): regex to match and group against principal.
- `user` (optional): regex to match against user name. If matched, it
  grants or denies the authorization based on the value of `allow`.
- `principal_to_user` (optional): replacement string to substitute against
  principal. If the result of the substitution is same as the user name, it
  grants or denies the authorization based on the value of `allow`.
- `allow` (required): boolean indicating whether a principal can be authorized
  as a user.

:::{note}
You would at least specify one criterion in a principal rule. If you specify
both criteria in a principal rule, it returns the desired conclusion when
either of criteria is satisfied.
:::

The following implements an exact matching of the full principal name for LDAP
and Kerberos authentication:

```json
{
  "principals": [
    {
      "principal": "(.*)",
      "principal_to_user": "$1",
      "allow": true
    },
    {
      "principal": "([^/]+)(/.*)?@.*",
      "principal_to_user": "$1",
      "allow": true
    }
  ]
}
```

If you want to allow users to use the exact same name as their Kerberos
principal name, and allow `alice` and `bob` to use a group principal named
as `group@example.net`, you can use the following rules.

```json
{
  "principals": [
    {
      "principal": "([^/]+)/?.*@example.net",
      "principal_to_user": "$1",
      "allow": true
    },
    {
      "principal": "group@example.net",
      "user": "alice|bob",
      "allow": true
    }
  ]
}
```

(system-file-auth-system-information)=

### System information rules

These rules specify which users can access the system information management
interface. System information access includes the following aspects:

- Read access to details such as Trino version, uptime of the node, and others
  from the `/v1/info` and `/v1/status` REST endpoints.
- Read access with the {doc}`system information functions </functions/system>`.
- Read access with the {doc}`/connector/system`.
- Write access to trigger {doc}`/admin/graceful-shutdown`.

The user is granted or denied access based on the first matching
rule read from top to bottom. If no rules are specified, all access to system
information is denied. If no rule matches, system access is denied. Each rule is
composed of the following fields:

- `role` (optional): regex to match against role. If matched, it
  grants or denies the authorization based on the value of `allow`.
- `user` (optional): regex to match against user name. If matched, it
  grants or denies the authorization based on the value of `allow`.
- `allow` (required): set of access permissions granted to user. Values:
  `read`, `write`

The following configuration provides and example:

```{literalinclude} system-information-access.json
:language: json
```

- All users with the `admin` role have read and write access to system
  information. This includes the ability to trigger
  {doc}`/admin/graceful-shutdown`.
- The user `alice` can read system information.
- All other users and roles are denied access to system information.

A fixed user can be set for management interfaces using the `management.user`
configuration property.  When this is configured, system information rules must
still be set to authorize this user to read or write to management information.
The fixed management user only applies to HTTP by default. To enable the fixed
user over HTTPS, set the `management.user.https-enabled` configuration
property.

(system-file-auth-authorization)=

### Authorization rules

These rules control the ability of how owner of schema, table or view can
be altered. These rules are applicable to commands like:

> ALTER SCHEMA name SET AUTHORIZATION ( user | USER user | ROLE role )
> ALTER TABLE name SET AUTHORIZATION ( user | USER user | ROLE role )
> ALTER VIEW name SET AUTHORIZATION ( user | USER user | ROLE role )

When these rules are present, the authorization is based on the first matching
rule, processed from top to bottom. If no rules match, the authorization is
denied.

Notice that in order to execute `ALTER` command on schema, table or view user requires `OWNERSHIP`
privilege.

Each authorization rule is composed of the following fields:

- `original_user` (optional): regex to match against the user requesting the
  authorization. Defaults to `.*`.
- `original_group` (optional): regex to match against group names of the
  requesting authorization. Defaults to `.*`.
- `original_role` (optional): regex to match against role names of the
  requesting authorization. Defaults to `.*`.
- `new_user` (optional): regex to match against the new owner user of the schema, table or view.
  By default it does not match.
- `new_role` (optional): regex to match against the new owner role of the schema, table or view.
  By default it does not match.
- `allow` (optional): boolean indicating if the authentication should be
  allowed. Defaults to `true`.

Notice that `new_user` and `new_role` are optional, however it is required to provide at least one of them.

The following example allows the `admin` role, to change owner of any schema, table or view
to any user, except to\`\`bob\`\`.

```{literalinclude} authorization.json
:language: json
```

(system-file-auth-system-information-1)=

(catalog-file-based-access-control)=

## Catalog-level access control files

You can create JSON files for individual catalogs that define authorization
rules specific to that catalog. To enable catalog-level access control files,
add a connector-specific catalog configuration property that sets the
authorization type to `FILE` and the `security.config-file` catalog
configuration property that specifies the JSON rules file.

For example, the following Iceberg catalog configuration properties use the
`rules.json` file for catalog-level access control:

```properties
iceberg.security=FILE
security.config-file=etc/catalog/rules.json
```

Catalog-level access control files are supported on a per-connector basis, refer
to the connector documentation for more information.

:::{note}
These rules do not apply to system-defined tables in the
`information_schema` schema.
:::

### Configure a catalog rules file

The configuration file is specified in JSON format. This file is composed of
the following sections, each of which is a list of rules that are processed in
order from top to bottom:

1. `schemas`
2. `tables`
3. `session_properties`

The user is granted the privileges from the first matching rule. All regexes
default to `.*` if not specified.

#### Schema rules

These rules govern who is considered an owner of a schema.

- `user` (optional): regex to match against user name.
- `group` (optional): regex to match against every user group the user belongs
  to.
- `schema` (optional): regex to match against schema name.
- `owner` (required): boolean indicating ownership.

#### Table rules

These rules govern the privileges granted on specific tables.

- `user` (optional): regex to match against user name.
- `group` (optional): regex to match against every user group the user belongs
  to.
- `schema` (optional): regex to match against schema name.
- `table` (optional): regex to match against table name.
- `privileges` (required): zero or more of `SELECT`, `INSERT`,
  `DELETE`, `UPDATE`, `OWNERSHIP`, `GRANT_SELECT`.
- `columns` (optional): list of column constraints.
- `filter` (optional): boolean filter expression for the table.
- `filter_environment` (optional): environment used during filter evaluation.

##### Column constraints

These constraints can be used to restrict access to column data.

- `name`: name of the column.
- `allow` (optional): if false, column can not be accessed.
- `mask` (optional): mask expression applied to column.
- `mask_environment` (optional): environment use during mask evaluation.

##### Filter environment and mask environment

These rules apply to `filter_environment` and `mask_environment`.

- `user` (optional): username for checking permission of subqueries in a mask.

:::{note}
`mask` can contain conditional expressions such as `IF` or `CASE`, which achieves conditional masking.
:::

#### Function rules

Each function rule is composed of the following fields:

- `user` (optional): regular expression to match against user name.
  Defaults to `.*`.
- `group` (optional): regular expression to match against group names.
  Defaults to `.*`.
- `schema` (optional): regular expression to match against schema name.
  Defaults to `.*`.
- `function` (optional): regular expression to match against function names.
  Defaults to `.*`.
- `privileges` (required): zero or more of `EXECUTE`, `GRANT_EXECUTE`.
- `function_kinds` (required): one or more of `AGGREGATE`, `SCALAR`,
  `TABLES`, `WINDOW`. When a user defines a rule for `AGGREGATE`, `SCALAR`
  or `WINDOW` functions, the `catalog` and `schema` fields are disallowed.

#### Session property rules

These rules govern who may set session properties.

- `user` (optional): regex to match against user name.
- `group` (optional): regex to match against every user group the user belongs
  to.
- `property` (optional): regex to match against session property name.
- `allow` (required): boolean indicating whether this session property may be
  set.

### Example

```json
{
  "schemas": [
    {
      "user": "admin",
      "schema": ".*",
      "owner": true
    },
    {
      "group": "finance|human_resources",
      "schema": "employees",
      "owner": true
    },
    {
      "user": "guest",
      "owner": false
    },
    {
      "schema": "default",
      "owner": true
    }
  ],
  "tables": [
    {
      "user": "admin",
      "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"]
    },
    {
      "user": "banned_user",
      "privileges": []
    },
    {
      "schema": "hr",
      "table": "employee",
      "privileges": ["SELECT"],
      "filter": "user = current_user"
    },
    {
      "schema": "default",
      "table": ".*",
      "privileges": ["SELECT"],
      "columns" : [
         {
            "name": "address",
            "allow": false
         },
         {
            "name": "ssn",
            "mask": "'XXX-XX-' + substring(credit_card, -4)",
            "mask_environment": {
              "user": "admin"
            }
         }
      ]
    }
  ],
  "session_properties": [
    {
      "property": "force_local_scheduling",
      "allow": true
    },
    {
      "user": "admin",
      "property": "max_split_size",
      "allow": true
    }
  ]
}
```
