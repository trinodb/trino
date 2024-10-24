# Faker connector

The Faker connector generates realistically looking random data matching a
specific schema.

## Configuration

To configure the Faker connector, create a catalog properties file
`etc/catalog/example.properties` with the following contents:

```text
connector.name=faker
```

## Examples

Faker generates data when reading from a table created in a catalog using this
connector. This makes it easy to fill an existing schema with random data, by
copying only the schema into a Faker catalog, and inserting the data back into
the original tables.

```sql
CREATE TABLE faker.default.customer (LIKE production.public.customer EXCLUDING PROPERTIES);

INSERT INTO production.public.customers
SELECT *
FROM faker.default.customers
WHERE
  born_at BETWEEN CURRENT_DATE - INTERVAL '150' YEAR AND CURRENT_DATE
  AND age_years BETWEEN 0 AND 150
LIMIT 100;
```

To generate more realistic data, choose specific generators by setting the
`generator` property on columns:

```sql
SHOW CREATE TABLE production.public.customers;

-- copy the output of the above query and add some properties:
CREATE TABLE faker.default.customer (
  id UUID NOT NULL,
  name VARCHAR NOT NULL WITH (generator = '#{Name.first_name} #{Name.last_name}'),
  address VARCHAR NOT NULL WITH (generator = '#{Address.fullAddress}'),
  born_at DATE,
  age_years INTEGER
);
```

See the Datafaker's documentation for more information about
[the expression](https://www.datafaker.net/documentation/expressions/) syntax
and [available providers](https://www.datafaker.net/documentation/providers/).

## Generators

A particular data generator is selected based on the column type.

For `CHAR`, `VARCHAR`, and `VARBINARY` column, the default generator uses the
`Lorem ipsum` placeholder text. Unbounded columns return a random sentence with
3 to 40 words.

To have more control over the format of the generated data, use the `generator`
column property. Some examples of valid generator expressions:

- `#{regexify '(a|b){2,3}'}`
- `#{regexify '\\.\\*\\?\\+'}`
- `#{bothify '????','false'}`
- `#{Name.first_name} #{Name.first_name} #{Name.last_name}`
- `#{number.number_between '1','10'}`

To test a generator expression, without having to recreate the table, use the
`random_string(expression)` function:

```sql
SELECT random_string('#{Name.first_name}')
```

Generator expressions cannot be used for non-character-based columns. To limit
their data range, specify constraints in the `WHERE` clause.

## Number of generated rows

By default, the connector generates 100 rows for every table. To control how
many rows are generated for a table, use the `LIMIT` clause in the query. A
default limit can be set using the `default_limit` table, or schema property or
in the connector configuration file, using the `faker.default-limit` property.

## Null values

For columns without the `NOT NULL` constraint, null values are generated using
the default probability of 50%. It can be modified using the `null_probability`
property set for a column, table, or schema. The default value of 0.5 can be
also modified in the connector configuration file, by using the
`faker.null-probability` property.

(faker-type-mapping)=
## Type mapping

The Faker connector generates data itself, so no mapping is required.

It supports all built-in Trino data types, except for:
- structural types: arrays, maps, rows
- JSON
- Geometry
- HyperLogLog and all digest types

To generate data using these complex types, data from column of primitive types
can be combined.

(faker-sql-support)=
## SQL support

The connector provides {ref}`globally available <sql-globally-available>` and
{ref}`read operation <sql-read-operations>` statements to generate data.

To define the schema for generating data, it supports the following features:

- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`

## Limitations

- Generated data is not deterministic. There is no way to specify a seed for
  the random generator.
- All schema objects created in this connector are not persisted, and are
  stored in memory only.
- It is not possible to choose the locale used by the Datafaker's generators.
