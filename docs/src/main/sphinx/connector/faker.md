# Faker connector

The Faker connector generates random data matching a defined structure. It uses
the [Datafaker](https://www.datafaker.net/) library to make the generated data
more realistic.

Use the connector to test and learn SQL queries without the need for a fixed,
imported dataset, or to populate another data source with large and realistic
test data. This allows testing the performance of applications processing data,
including Trino itself, and application user interfaces accessing the data.

## Configuration

Create a catalog properties file that specifies the Faker connector by setting
the `connector.name` to `faker`.

For example, to generate data in the `generator` catalog, create the file
`etc/catalog/generator.properties`.

```text
connector.name=faker
faker.null-probability=0.1
faker.default-limit=1000
faker.locale=pl
```

Create tables in the `default` schema, or create different schemas first. Tables
in the catalog only exist as definition and do not hold actual data. Any query
reading from tables returns random, but deterministic data. As a result,
repeated invocation of a query returns identical data. See [](faker-usage) for
more examples.

Schemas and tables in a catalog are not persisted, and are stored in the memory
of the coordinator only. They need to be recreated every time after restarting
the coordinator.

The following table details all general configuration properties:

:::{list-table} Faker configuration properties
:widths: 25, 75
:header-rows: 1

* - Property name
  - Description
* - `faker.null-probability`
  - Default probability of a value created as `null` for any column in any table
    that allows them. Defaults to `0.5`.
* - `faker.default-limit`
  - Default number of rows in a table. Defaults to `1000`.
* - `faker.locale`
  - Default locale for generating character-based data, specified as a IETF BCP
    47 language tag string. Defaults to `en`.
:::

The following table details all supported schema properties. If they're not
set, values from corresponding configuration properties are used.

:::{list-table} Faker schema properties
:widths: 25, 75
:header-rows: 1

* - Property name
  - Description
* - `null_probability`
  - Default probability of a value created as `null` in any column that allows
    them, in any table of this schema.
* - `default_limit`
  - Default number of rows in a table.
:::

The following table details all supported table properties. If they're not set,
values from corresponding schema properties are used.

:::{list-table} Faker table properties
:widths: 25, 75
:header-rows: 1

* - Property name
  - Description
* - `null_probability`
  - Default probability of a value created as `null` in any column that allows
    `null` in the table.
* - `default_limit`
  - Default number of rows in the table.
:::

The following table details all supported column properties.

:::{list-table} Faker column properties
:widths: 25, 75
:header-rows: 1

* - Property name
  - Description
* - `null_probability`
  - Default probability of a value created as `null` in the column. Defaults to
    the `null_probability` table or schema property, if set, or the
    `faker.null-probability` configuration property.
* - `generator`
  - Name of the Faker library generator used to generate data for the column.
    Only valid for columns of a character-based type. Defaults to a 3 to 40 word
    sentence from the
    [Lorem](https://javadoc.io/doc/net.datafaker/datafaker/latest/net/datafaker/providers/base/Lorem.html)
    provider.
* - `min`
  - Minimum generated value (inclusive). Cannot be set for character-based type
    columns.
* - `max`
  - Maximum generated value (inclusive). Cannot be set for character-based type
    columns.
* - `allowed_values`
  - List of allowed values. Cannot be set together with the `min`, or `max`
    properties.
:::

### Character types

Faker supports the following character types:

- `CHAR`
- `VARCHAR`
- `VARBINARY`

Columns of those types use a generator producing the [Lorem
ipsum](https://en.wikipedia.org/wiki/Lorem_ipsum) placeholder text. Unbounded
columns return a random sentence with 3 to 40 words.

To have more control over the format of the generated data, use the `generator`
column property. Some examples of valid generator expressions:

- `#{regexify '(a|b){2,3}'}`
- `#{regexify '\\.\\*\\?\\+'}`
- `#{bothify '????','false'}`
- `#{Name.first_name} #{Name.first_name} #{Name.last_name}`
- `#{number.number_between '1','10'}`

See the Datafaker's documentation for more information about
[the expression](https://www.datafaker.net/documentation/expressions/) syntax
and [available providers](https://www.datafaker.net/documentation/providers/).

:::{function} random_string(expression_string) -> string

Create a random output `string` with the provided input `expression_string`. The
expression must use the [syntax from
Datafaker](https://www.datafaker.net/documentation/expressions/).

Use the `random_string` function from the `default` schema of the `generator`
catalog to test a generator expression:

```sql
SELECT generator.default.random_string('#{Name.first_name}');
```
:::

### Non-character types

Faker supports the following non-character types:

- `BIGINT`
- `INTEGER` or `INT`
- `SMALLINT`
- `TINYINT`
- `BOOLEAN`
- `DATE`
- `DECIMAL`
- `REAL`
- `DOUBLE`
- `INTERVAL DAY TO SECOND`
- `INTERVAL YEAR TO MONTH`
- `TIMESTAMP` and `TIMESTAMP(P)`
- `TIMESTAMP WITH TIME ZONE` and `TIMESTAMP(P) WITH TIME ZONE`
- `TIME` and `TIME(P)`
- `TIME WITH TIME ZONE` and `TIME(P) WITH TIME ZONE`
- `IPADDRESS`
- `UUID`

You can not use generator expressions for non-character-based columns. To limit
their data range, set the `min` and/or `max` column properties - see
[](faker-usage).

### Unsupported types

Faker does not support the following data types:

- Structural types `ARRAY`, `MAP`, and `ROW`
- `JSON`
- Geometry
- HyperLogLog and all digest types

To generate data using these complex types, data from column of primitive types
can be combined, like in the following example:

```sql
CREATE TABLE faker.default.prices (
  currency VARCHAR NOT NULL WITH (generator = '#{Currency.code}'),
  price DECIMAL(8,2) NOT NULL WITH (min = '0')
);

SELECT JSON_OBJECT(KEY currency VALUE price) AS complex
FROM faker.default.prices
LIMIT 3;
```

Running the queries returns data similar to the following result:

```text
      complex
-------------------
 {"TTD":924657.82}
 {"MRO":968292.49}
 {"LTL":357773.63}
(3 rows)
```

### Number of generated rows

By default, the connector generates 1000 rows for every table. To control how
many rows are generated for a table, use the `LIMIT` clause in the query. A
default limit can be set using the `default_limit` table, or schema property or
in the connector configuration file, using the `faker.default-limit` property.
Use a limit value higher than the configured default to return more rows.

### Null values

For columns without a `NOT NULL` constraint, `null` values are generated using
the default probability of 50%. It can be modified using the `null_probability`
property set for a column, table, or schema. The default value of 0.5 can be
also modified in the catalog configuration file, by using the
`faker.null-probability` property.

(faker-type-mapping)=
## Type mapping

The Faker connector generates data itself, so no mapping is required.

(faker-sql-support)=
## SQL support

The connector provides [globally available](sql-globally-available) and [read
operation](sql-read-operations) statements to generate data.

To define the schema for generating data, it supports the following features:

- [](/sql/create-table)
- [](/sql/create-table-as)
- [](/sql/drop-table)
- [](/sql/create-schema)
- [](/sql/drop-schema)

(faker-usage)=
## Usage

Faker generates data when reading from a table created in a catalog using this
connector. This makes it easy to fill an existing schema with random data, by
copying only the schema into a Faker catalog, and inserting the data back into
the original tables.

Using the catalog definition from Configuration you can proceed with the
following steps.

Create a table with the same columns as in the table to populate with random
data. Exclude all properties, because the Faker connector doesn't support the
same table properties as other connectors.

```sql
CREATE TABLE generator.default.customer (LIKE production.public.customer EXCLUDING PROPERTIES);
```

Insert random data into the original table, by selecting it from the
`generator` catalog. Data generated by the Faker connector for columns of
non-character types cover the whole range of that data type. Set the `min` and
`max` column properties, to adjust the generated data as desired. The following
example ensures that date of birth and age in years are related and realistic
values.

Start with getting the complete definition of
a table:

```sql
SHOW CREATE TABLE production.public.customers;
```

Modify the output of the previous query and add some column properties.

```sql
CREATE TABLE generator.default.customer (
  id UUID NOT NULL,
  name VARCHAR NOT NULL,
  address VARCHAR NOT NULL,
  born_at DATE WITH (min = '1900-01-01', max = '2025-01-01'),
  age_years INTEGER WITH (min = '0', max = '150'),
  group_id INTEGER WITH (allowed_values = ARRAY['10', '32', '81'])
);
```

```sql
INSERT INTO production.public.customers
SELECT *
FROM generator.default.customers
LIMIT 100;
```

To generate even more realistic data, choose specific generators by setting the
`generator` property on columns.

```sql
CREATE TABLE generator.default.customer (
  id UUID NOT NULL,
  name VARCHAR NOT NULL WITH (generator = '#{Name.first_name} #{Name.last_name}'),
  address VARCHAR NOT NULL WITH (generator = '#{Address.fullAddress}'),
  born_at DATE WITH (min = '1900-01-01', max = '2025-01-01'),
  age_years INTEGER WITH (min = '0', max = '150'),
  group_id INTEGER WITH (allowed_values = ARRAY['10', '32', '81'])
);
```

## Limitations

* It is not possible to choose the locale used by the Datafaker's generators.
