# ALTER TABLE

## Synopsis

```text
ALTER TABLE [ IF EXISTS ] name RENAME TO new_name
ALTER TABLE [ IF EXISTS ] name ADD COLUMN [ IF NOT EXISTS ] column_name data_type
  [ NOT NULL ] [ COMMENT comment ]
  [ WITH ( property_name = expression [, ...] ) ]
ALTER TABLE [ IF EXISTS ] name DROP COLUMN [ IF EXISTS ] column_name
ALTER TABLE [ IF EXISTS ] name RENAME COLUMN [ IF EXISTS ] old_name TO new_name
ALTER TABLE [ IF EXISTS ] name ALTER COLUMN column_name SET DATA TYPE new_type
ALTER TABLE [ IF EXISTS ] name ALTER COLUMN column_name DROP NOT NULL
ALTER TABLE name SET AUTHORIZATION ( user | USER user | ROLE role )
ALTER TABLE name SET PROPERTIES property_name = expression [, ...]
ALTER TABLE name EXECUTE command [ ( parameter => expression [, ... ] ) ]
    [ WHERE expression ]
```

## Description

Change the definition of an existing table.

The optional `IF EXISTS` clause, when used before the table name, causes the
error to be suppressed if the table does not exist.

The optional `IF EXISTS` clause, when used before the column name, causes the
error to be suppressed if the column does not exist.

The optional `IF NOT EXISTS` clause causes the error to be suppressed if the
column already exists.

(alter-table-set-properties)=

### SET PROPERTIES

The `ALTER TABLE SET PROPERTIES`  statement followed by a number of
`property_name` and `expression` pairs applies the specified properties and
values to a table. Ommitting an already-set property from this statement leaves
that property unchanged in the table.

A property in a `SET PROPERTIES` statement can be set to `DEFAULT`, which
reverts its value back to the default in that table.

Support for `ALTER TABLE SET PROPERTIES` varies between
connectors, as not all connectors support modifying table properties.

(alter-table-execute)=

### EXECUTE

The `ALTER TABLE EXECUTE` statement followed by a `command` and
`parameters` modifies the table according to the specified command and
parameters. `ALTER TABLE EXECUTE` supports different commands on a
per-connector basis.

You can use the `=>` operator for passing named parameter values. The left side
is the name of the parameter, the right side is the value being passed.

Executable commands are contributed by connectors, such as the `optimize`
command provided by the [Hive](hive-alter-table-execute), [Delta
Lake](delta-lake-alter-table-execute), and
[Iceberg](iceberg-alter-table-execute) connectors. For example, a user observing
many small files in the storage of a table called `test_table` in the `test`
schema of the `example` catalog, can use the `optimize` command to merge all
files below the `file_size_threshold` value. The result is fewer, but larger
files, which typically results in higher query performance on the data in the
files:

```
ALTER TABLE example.test.test_table EXECUTE optimize(file_size_threshold => '16MB')
```

## Examples

Rename table `users` to `people`:

```
ALTER TABLE users RENAME TO people;
```

Rename table `users` to `people` if table `users` exists:

```
ALTER TABLE IF EXISTS users RENAME TO people;
```

Add column `zip` to the `users` table:

```
ALTER TABLE users ADD COLUMN zip varchar;
```

Add column `zip` to the `users` table if table `users` exists and column `zip`
not already exists:

```
ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS zip varchar;
```

Drop column `zip` from the `users` table:

```
ALTER TABLE users DROP COLUMN zip;
```

Drop column `zip` from the `users` table if table `users` and column `zip`
exists:

```
ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS zip;
```

Rename column `id` to `user_id` in the `users` table:

```
ALTER TABLE users RENAME COLUMN id TO user_id;
```

Rename column `id` to `user_id` in the `users` table if table `users` and column
`id` exists:

```
ALTER TABLE IF EXISTS users RENAME column IF EXISTS id to user_id;
```

Change type of column `id` to `bigint` in the `users` table:

```
ALTER TABLE users ALTER COLUMN id SET DATA TYPE bigint;
```

Drop a not null constraint on `id` column in the `users` table:

```
ALTER TABLE users ALTER COLUMN id DROP NOT NULL;
```

Change owner of table `people` to user `alice`:

```
ALTER TABLE people SET AUTHORIZATION alice
```

Allow everyone with role public to drop and alter table `people`:

```
ALTER TABLE people SET AUTHORIZATION ROLE PUBLIC
```

Set table properties (`x = y`) in table `people`:

```
ALTER TABLE people SET PROPERTIES x = 'y';
```

Set multiple table properties (`foo = 123` and `foo bar = 456`) in
table `people`:

```
ALTER TABLE people SET PROPERTIES foo = 123, "foo bar" = 456;
```

Set table property `x` to its default value in table\`\`people\`\`:

```
ALTER TABLE people SET PROPERTIES x = DEFAULT;
```


## See also

{doc}`create-table`
