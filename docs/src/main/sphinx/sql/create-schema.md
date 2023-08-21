# CREATE SCHEMA

## Synopsis

```text
CREATE SCHEMA [ IF NOT EXISTS ] schema_name
[ AUTHORIZATION ( user | USER user | ROLE role ) ]
[ WITH ( property_name = expression [, ...] ) ]
```

## Description

Create a new, empty schema. A schema is a container that
holds tables, views and other database objects.

The optional `IF NOT EXISTS` clause causes the error to be
suppressed if the schema already exists.

The optional `AUTHORIZATION` clause can be used to set the
owner of the newly created schema to a user or role.

The optional `WITH` clause can be used to set properties
on the newly created schema.  To list all available schema
properties, run the following query:

```
SELECT * FROM system.metadata.schema_properties
```

## Examples

Create a new schema `web` in the current catalog:

```
CREATE SCHEMA web
```

Create a new schema `sales` in the `hive` catalog:

```
CREATE SCHEMA hive.sales
```

Create the schema `traffic` if it does not already exist:

```
CREATE SCHEMA IF NOT EXISTS traffic
```

Create a new schema `web` and set the owner to user `alice`:

```
CREATE SCHEMA web AUTHORIZATION alice
```

Create a new schema `web`, set the `LOCATION` property to `/hive/data/web`
and set the owner to user `alice`:

```
CREATE SCHEMA web AUTHORIZATION alice WITH ( LOCATION = '/hive/data/web' )
```

Create a new schema `web` and allow everyone to drop schema and create tables
in schema `web`:

```
CREATE SCHEMA web AUTHORIZATION ROLE PUBLIC
```

Create a new schema `web`, set the `LOCATION` property to `/hive/data/web`
and allow everyone to drop schema and create tables in schema `web`:

```
CREATE SCHEMA web AUTHORIZATION ROLE PUBLIC WITH ( LOCATION = '/hive/data/web' )
```

## See also

{doc}`alter-schema`, {doc}`drop-schema`
