# Faker connector

The Faker connector generates random data matching a defined structure. It uses
the [Datafaker](https://www.datafaker.net/) library to make the generated data
more realistic. Use the connector to populate another data source with large
and realistic test data. This allows testing performance of applications
processing data, including Trino itself, and application user interfaces.

## Configuration

Create a catalog properties file that specifies the Faker connector by setting
the `connector.name` to `faker`.

For example, to generate data in the `generator` catalog, create the file
`etc/catalog/generator.properties`.

```text
connector.name=faker
faker.null-probability=0.1
faker.default-limit=1000
```

Create tables in the `default` schema, or create different schemas first.
Reading from tables in this catalog return random data. See [](faker-usage) for
more examples.

Schema objects created in this connector are not persisted, and are stored in
memory only. They need to be recreated every time after restarting the
coordinator.

The following table details all general configuration properties:

:::{list-table} Faker configuration properties
:widths: 35, 55, 10
:header-rows: 1

* - Property name
  - Description
  - Default
* - `faker.null-probability`
  - Default null probability for any column in any table that allows them.
  - `0.5`
* - `faker.default-limit`
  - Default number of rows for each table, when the LIMIT clause is not
    specified in the query.
  - `1000`
:::

The following table details all supported schema properties. If they're not
set, values from corresponding configuration properties are used.

:::{list-table} Faker schema properties
:widths: 35, 65
:header-rows: 1

* - Property name
  - Description
* - `null_probability`
  - Default probability of null values in any column that allows them, in any
    table of this schema.
* - `default_limit`
  - Default limit of rows returned from any table in this schema, if not
    specified in the query.
:::

The following table details all supported table properties. If they're not set,
values from corresponding schema properties are used.

:::{list-table} Faker table properties
:widths: 35, 65
:header-rows: 1

* - Property name
  - Description
* - `null_probability`
  - Default probability of null values in any column in this table that allows
    them.
* - `default_limit`
  - Default limit of rows returned from this table if not specified in the
    query.
:::

The following table details all supported column properties.

:::{list-table} Faker column properties
:widths: 20, 40, 40
:header-rows: 1

* - Property name
  - Description
  - Default
* - `null_probability`
  - Default probability of null values in any column in this table that allows them.
  - Defaults to the `null_probability` table or schema property, if set, or the
    `faker.null-probability` configuration property.
* - `generator`
  - Name of the Faker library generator used to generate data for this column.
    Only valid for columns of a character based type.
  - Defaults to a 3 to 40 word sentence from the
    [Lorem](https://javadoc.io/doc/net.datafaker/datafaker/latest/net/datafaker/providers/base/Lorem.html)
    provider.
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
their data range, specify constraints in the `WHERE` clause.

### Unsupported types

Faker does not support the following data types:

- structural types: `ARRAY`, `MAP`, `ROW`
- `JSON`
- Geometry
- HyperLogLog and all digest types

To generate data using these complex types, data from column of primitive types
can be combined, like in the following example.

```sql
CREATE TABLE faker.default.prices (
  currency VARCHAR NOT NULL WITH (generator = '#{Currency.code}'),
  price DECIMAL(8,2) NOT NULL
);

SELECT JSON_OBJECT(KEY currency VALUE price) AS complex
FROM faker.default.prices
WHERE price > 0
LIMIT 3;
```

Executing these queries should return data structured like this:

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

### Null values

For columns without a `NOT NULL` constraint, null values are generated using
the default probability of 50%. It can be modified using the `null_probability`
property set for a column, table, or schema. The default value of 0.5 can be
also modified in the connector configuration file, by using the
`faker.null-probability` property.

(faker-type-mapping)=
## Type mapping

The Faker connector generates data itself, so no mapping is required.

(faker-sql-support)=
## SQL support

The connector provides {ref}`globally available <sql-globally-available>` and
{ref}`read operation <sql-read-operations>` statements to generate data.

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
non-character types cover the whole range of that data type. Add constraints to
adjust the data as desired. The following example ensures that date of birth
and age in years are related and realistic values.

```sql
INSERT INTO production.public.customers
SELECT *
FROM generator.default.customers
WHERE
  born_at BETWEEN CURRENT_DATE - INTERVAL '150' YEAR AND CURRENT_DATE
  AND age_years BETWEEN 0 AND 150
LIMIT 100;
```

To generate even more realistic data, choose specific generators by setting the
`generator` property on columns. Start with getting the complete definition of
a table:

```sql
SHOW CREATE TABLE production.public.customers;
```

Modify the output of the previous query and add some column properties.

```sql
CREATE TABLE generator.default.customer (
  id UUID NOT NULL,
  name VARCHAR NOT NULL WITH (generator = '#{Name.first_name} #{Name.last_name}'),
  address VARCHAR NOT NULL WITH (generator = '#{Address.fullAddress}'),
  born_at DATE,
  age_years INTEGER
);
```

## Limitations

- Generated data is not deterministic. There is no way to specify a seed for
  the random generator. The same query reading from catalogs using this
  connector, executed multiple times, returns different results each time.
- It is not possible to choose the locale used by the Datafaker's generators.
