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
reading from tables returns random, but deterministic data. Every generated
value only depends on the `seed` table property, its column, row number, and the
`faker.locale` catalog property. Values of columns with the `references`
property also depend on the referenced table. As a result, repeated invocation
of a query returns identical data, and different queries reading the same table,
like self-joins, return matching rows, no matter which columns they read, or how
they filter or limit the rows. Some generator expressions, like
`#{Date.birthday}`, depend on the current date or other sources outside of the
table, so their values can change between queries. See [](faker-usage) for more
examples.

Schemas, tables, and views in a catalog are not persisted, and are stored in the
memory of the coordinator only. They need to be recreated every time after
restarting the coordinator.

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
  - Default locale for generating character-based data, specified as an IETF BCP
    47 language tag string. Defaults to `en`.
* - `faker.sequence-detection-enabled`
  - If true, when creating a table using existing data, columns with the number
    of distinct values close to the number of rows are treated as sequences.
    Defaults to `true`.
* - `faker.dictionary-detection-enabled`
  - If true, when creating a table using existing data, columns with a low
    number of distinct values are treated as dictionaries, and get
    the `allowed_values` column property populated with random values.
    Defaults to `true`.
* - `faker.generator-detection-enabled`
  - If true, when creating a table, `CHAR` and `VARCHAR` columns with a
    recognized name, like `email` or `city`, get the `generator` column
    property set, see [generator detection](faker-generator-detection).
    Defaults to `false`.
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
* - `sequence_detection_enabled`
  - If true, when creating a table using existing data, columns with the number
    of distinct values close to the number of rows are treated as sequences.
    Defaults to `true`.
* - `dictionary_detection_enabled`
  - If true, when creating a table using existing data, columns with a low
    number of distinct values are treated as dictionaries, and get
    the `allowed_values` column property populated with random values.
    Defaults to `true`.
* - `generator_detection_enabled`
  - If true, when creating a table, `CHAR` and `VARCHAR` columns with a
    recognized name, like `email` or `city`, get the `generator` column
    property set, see [generator detection](faker-generator-detection).
    Defaults to `false`.
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
* - `sequence_detection_enabled`
  - If true, when creating a table using existing data, columns with the number
    of distinct values close to the number of rows are treated as sequences.
    Defaults to `true`.
* - `dictionary_detection_enabled`
  - If true, when creating a table using existing data, columns with a low
    number of distinct values are treated as dictionaries, and get
    the `allowed_values` column property populated with random values.
    Defaults to `true`.
* - `generator_detection_enabled`
  - If true, when creating a table, `CHAR` and `VARCHAR` columns with a
    recognized name, like `email` or `city`, get the `generator` column
    property set, see [generator detection](faker-generator-detection).
    Defaults to `false`.
* - `seed`
  - Seed for generating data in the table. Tables with the same seed, column
    definitions, and `null_probability` property, in catalogs with the same
    `faker.locale` property, return identical data. Tables without the
    `null_probability` property use the one of their schema. Defaults to a
    value derived from the schema and table name, so renaming a table without
    this property changes its data, except for columns with the `step`
    property and without nulls.
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
    For columns of types other than character-based types, the generated text
    must be a valid value of the column type, in the same format as the `min`
    and `max` properties. Cannot be set for `ROW`, `ARRAY`, `MAP`, or `JSON`
    columns, or together with the `min`, `max`, `step`, or `allowed_values`
    properties. Defaults to a 3 to 40 word sentence from the
    [Lorem](https://javadoc.io/doc/net.datafaker/datafaker/latest/net/datafaker/providers/base/Lorem.html)
    provider. Generated values longer than the column length are truncated.
* - `min`
  - Minimum generated value (inclusive). Defaults to the minimum value of the
    data type, except for `DATE`, `TIMESTAMP`, and `TIMESTAMP WITH TIME ZONE`
    columns, where it defaults to `1970-01-01` in UTC, unless `max` is set to
    before `1970-01-01`. Values of date and time columns can't be more precise
    than the column type. Cannot be set for character-based type columns.
* - `max`
  - Maximum generated value (inclusive). Defaults to the maximum value of the
    data type, except for `DATE`, `TIMESTAMP`, and `TIMESTAMP WITH TIME ZONE`
    columns, where values are generated before `2100-01-01` in UTC, unless `min`
    is set to `2100-01-01` or later. Values of date and time columns can't be
    more precise than the column type. Cannot be set for character-based type
    columns.
* - `allowed_values`
  - List of allowed values. Values of date and time columns can't be more
    precise than the column type. Cannot be set together with the `min`, or
    `max` properties.
* - `step`
  - If set, generate sequential values with this step, which must be finite and
    greater than zero. For date and time columns set this to a duration, which
    can't be shorter than the precision of the column type, like `1d` for `DATE`
    columns, or `1ms` for `TIMESTAMP(3)` columns.
    Without `min`, sequences of `DATE`, `TIMESTAMP`, and `TIMESTAMP WITH TIME
    ZONE` columns start at `1970-01-01`, and continue past `2100-01-01` when
    `max` isn't set. Can only be set for numeric, date and time, and interval
    type columns.
* - `references`
  - Column of another or the same table, in the `table.column` or
    `schema.table.column` format, so names that contain periods can't be
    referenced. If set, generate only values that exist in the
    [referenced column](faker-references). Cannot be set together with the
    `min`, `max`, `step`, `allowed_values`, or `generator` properties.
* - `distribution`
  - Distribution of generated values, one of `uniform`, `normal`, or `zipf`.
    Defaults to `uniform`. The `normal` and `zipf` values cannot be set
    together with the `step` or `generator` properties, and the `normal` value
    cannot be set together with the `allowed_values` property. See
    [](faker-distribution).
:::

(faker-references)=
### Column references

To generate related tables, like a foreign key that always matches a primary
key, set the `references` column property. It names a column of the same type
in another table, or in the same table. For every row, Faker picks a random row
of the referenced table, up to its `default_limit`, and uses the value the
referenced column has in that row:

```sql
CREATE TABLE generator.default.customer (
  id UUID NOT NULL,
  name VARCHAR NOT NULL WITH (generator = '#{Name.first_name}')
)
WITH (default_limit = 100);

CREATE TABLE generator.default.orders (
  id UUID NOT NULL,
  customer_id UUID NOT NULL WITH (references = 'customer.id')
);
```

Every order matches a customer, unless a query reads fewer rows from the
`customer` table than its `default_limit`. Columns pick rows independently, so
multiple columns referencing the same table can't form a composite foreign key.
The referenced column can itself reference another column.

The referencing column has `null` values according to its own
`null_probability` property. Rows where the referenced column is `null` are
skipped, so the referenced column can't have the `null_probability` property set
to `1`, and the referenced table can't be empty. Reading the referencing table
fails if the referenced table or column is dropped, or changes its type. It also
fails if all rows of the referenced column are `null`, unless the referencing
column has the `null_probability` property set to `1`.

Reading a referencing column doesn't check access to the referenced table, so
it can return values that the referenced table generates.

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

(faker-generator-detection)=
### Generator detection

When the `generator_detection_enabled` table or schema property, or the
`faker.generator-detection-enabled` configuration property is set to `true`,
Faker sets the `generator` column property for `CHAR` and `VARCHAR` columns
with a recognized name. This applies to all tables created with
[](/sql/create-table), including when using the `LIKE` clause, and with
[](/sql/create-table-as). Detected generators are included in the output of
`SHOW CREATE TABLE`. To change them, edit that output, and recreate the table.

Column names are compared ignoring case, underscores, hyphens, and spaces, so
`first_name`, `FirstName`, and `first-name` are all recognized. Columns with
the `generator` or `allowed_values` property already set are not changed.
Faker truncates generated values to the column length, so to keep values like
email addresses valid, a bounded column only gets a generator if its length is
at least the minimum length listed below.

When creating a table using existing data, columns with a detected generator
are excluded from dictionary detection, because generators produce more
realistic values.

:::{list-table} Recognized column names
:widths: 40, 45, 15
:header-rows: 1

* - Column names
  - Generator
  - Minimum length
* - `email`, `email_address`
  - `#{Internet.emailAddress}`
  - 60
* - `first_name`, `given_name`
  - `#{Name.first_name}`
  - 30
* - `last_name`, `family_name`, `surname`
  - `#{Name.last_name}`
  - 50
* - `full_name`
  - `#{Name.first_name} #{Name.last_name}`
  - 60
* - `username`
  - `#{Internet.username}`
  - 60
* - `street_address`
  - `#{Address.streetAddress}`
  - 100
* - `city`
  - `#{Address.city}`
  - 60
* - `country`
  - `#{Address.country}`
  - 60
* - `country_code`
  - `#{Country.countryCode2}`
  - 2
* - `postal_code`, `postcode`, `zip`, `zip_code`
  - `#{Address.zipCode}`
  - 10
* - `company`, `company_name`
  - `#{Company.name}`
  - 120
* - `job_title`
  - `#{Job.title}`
  - 100
* - `website`, `url`
  - `https://#{Internet.domainName}`
  - 150
* - `currency`, `currency_code`
  - `#{Currency.code}`
  - 3
* - `iban`
  - `#{Finance.iban}`
  - 34
* - `ip_address`
  - `#{Internet.ipV4Address}`
  - 15
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
- `INTERVAL DAY TO SECOND` with a
  [fractional-seconds precision](interval-fractional-seconds-precision) of up
  to 6
- `INTERVAL YEAR TO MONTH`
- `TIMESTAMP` and `TIMESTAMP(P)`
- `TIMESTAMP WITH TIME ZONE` and `TIMESTAMP(P) WITH TIME ZONE`
- `TIME` and `TIME(P)`
- `TIME WITH TIME ZONE` and `TIME(P) WITH TIME ZONE`
- `ROW`
- `ARRAY`
- `MAP`
- `IPADDRESS`
- `UUID`

To limit the range of generated values, set the `min` and `max` column
properties - see [usage examples](faker-usage). To generate more realistic
values, set the `generator` column property to an expression that produces
valid values of the column type, like in the following example:

```sql
CREATE TABLE generator.default.customer (
  rating INTEGER NOT NULL WITH (generator = '#{Number.numberBetween ''1'',''6''}'),
  birthday DATE NOT NULL WITH (generator = '#{Date.birthday ''yyyy-MM-dd''}'),
  balance DECIMAL(10,2) NOT NULL WITH (generator = '#{Commerce.price}'),
  last_login_ip IPADDRESS NOT NULL WITH (generator = '#{Internet.ipV4Address}')
);
```

Day-time intervals support up to six fractional-second digits. Their `min`,
`max`, `step`, and `allowed_values` properties use seconds; year-month interval
properties use months. Generated values respect the qualifier, leading
precision, and fractional precision. Intervals with more than six
fractional-second digits are not supported and are rejected when a table is
created.

Every `ARRAY` and `MAP` value has up to five elements, generated the same way as
values of a column of the element, key, or value type. Array elements and map
values are never `null`, unless Faker does not generate data for their type.
Duplicate map keys are skipped, so maps can have fewer entries, like at most two
for `BOOLEAN` keys. Keys that would be `null` or contain `null` are skipped too,
so maps are always empty when Faker does not generate data for their key type.

### Unsupported types

Faker does not support `INTERVAL DAY TO SECOND` with a fractional-seconds
precision greater than 6.

Faker can create columns of the `JSON` type, and of other types not listed
above, like the geometry, HyperLogLog, and digest types, but does not generate
data for them. Every non-null `JSON` value is the JSON `null` literal, and every
value of other types is `null`, even in `NOT NULL` columns.

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

(faker-distribution)=
### Value distribution

By default, all values in a column are equally likely. To generate skewed data,
like a few customers placing most of the orders, set the `distribution` column
property:

- `uniform` - every value is equally likely.
- `normal` - values follow a normal distribution, also called a bell curve,
  with the mean in the middle between `min` and `max`, and the standard
  deviation equal to one sixth of that range. Requires finite `min` and `max`
  properties, and is supported for numeric, `DATE`, and `TIMESTAMP` columns.
- `zipf` - values follow a Zipf distribution, where the n-th value is n times
  less likely than the first one. With the `allowed_values` property, values
  are ranked in the order they are listed, and a value listed more than once
  gets the combined frequency of all its ranks. Otherwise, values are ranked by
  their distance from `min`, so `min` is the most frequent value. Without the
  `allowed_values` property, this requires the `min` and `max` properties, and
  is supported for `BIGINT`, `INTEGER`, `SMALLINT`, `TINYINT`, `DATE`, and
  `TIMESTAMP` columns.

For `TIMESTAMP` columns with a precision higher than 6, the distribution applies
to whole microseconds, and fractions of a microsecond are uniform.

The following example creates a table where the first few products appear in
most orders, and order amounts cluster around 50:

```sql
CREATE TABLE generator.default.orders (
  product_id INTEGER NOT NULL
    WITH (min = '1', max = '10000', distribution = 'zipf'),
  amount DECIMAL(8,2) NOT NULL
    WITH (min = '0', max = '100', distribution = 'normal')
);
```

(faker-type-mapping)=
## Type mapping

The Faker connector generates data itself, so no mapping is required.

(faker-sql-support)=
## SQL support

The connector provides [globally available](sql-globally-available) and [read
operation](sql-read-operations) statements to generate data.

To define the schema for generating data, it supports the following features:

- [](/sql/create-table)
- [](/sql/create-table-as), see also [](faker-statistics)
- [](/sql/drop-table)
- [](/sql/create-schema)
- [](/sql/drop-schema)
- [](sql-view-management)

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
non-character types cover the whole range of that data type, except `DATE`,
`TIMESTAMP`, and `TIMESTAMP WITH TIME ZONE` columns, which default to values
from `1970-01-01` (inclusive) to `2100-01-01` (exclusive), in UTC. Sequences
of columns with the `step` property continue past `2100-01-01`, unless `max` is
set. Set the `min` and `max` column properties, to adjust the generated data as
desired. The
following example ensures that date of birth and age in years are realistic
values. Faker generates every column independently, so these values are not
related to each other.

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

To make one column depend on another, compute it in a view over the Faker
table, and select data from the view instead of the table:

```sql
CREATE VIEW generator.default.customer_view AS
SELECT
  id,
  name,
  address,
  born_at,
  date_diff('year', born_at, current_date) AS age_years,
  group_id
FROM generator.default.customer;
```

(faker-statistics)=
### Using existing data statistics

The Faker connector automatically sets the `default_limit` table property, and
the `min`, `max`, and `null_probability` column properties, based on statistics
collected by scanning existing data read by Trino from the data source. The
connector uses these statistics to be able to generate data that is more similar
to the original data set, without using any of that data:


```sql
CREATE TABLE generator.default.customer AS
SELECT *
FROM production.public.customer
WHERE created_at > CURRENT_DATE - INTERVAL '1' YEAR;
```

Instead of using range, or other predicates, tables can be sampled,
see [](tablesample).

When the `SELECT` statement doesn't contain a `WHERE` clause, a shorter notation
can be used:

```sql
CREATE TABLE generator.default.customer AS TABLE production.public.customer;
```

The Faker connector detects sequence columns, which are integer column with the
number of distinct values almost equal to the number of rows in the table. For
such columns, Faker sets the `step` column property to 1.

Sequence detection can be turned off using the `sequence_detection_enabled`
table, or schema property or in the connector configuration file, using the
`faker.sequence-detection-enabled` property.

The Faker connector detects dictionary columns, which are columns of numeric,
date and time, `CHAR`, and `VARCHAR` types with the number of distinct values
lower or equal to 1000. For such columns, Faker generates a list of random
values to choose from, and saves it in the `allowed_values` column property.
Faker limits the length of these generated `CHAR` and `VARCHAR` values to the
size of the longest value in the original data, and to 1000 characters.

Dictionary detection can be turned off using the `dictionary_detection_enabled`
table, or schema property or in the connector configuration file, using
the `faker.dictionary-detection-enabled` property.

For example, copy the `orders` table from the TPC-H connector with
statistics, using the following query:

```sql
CREATE TABLE generator.default.orders AS TABLE tpch.tiny.orders;
```

Inspect the schema of the table created by the Faker connector:
```sql
SHOW CREATE TABLE generator.default.orders;
```

The table schema should contain additional column and table properties.
```
CREATE TABLE generator.default.orders (
   orderkey bigint WITH (max = '60000', min = '1', null_probability = 0E0, step = '1'),
   custkey bigint WITH (allowed_values = ARRAY['153','662','1453','63','784', ..., '1493','657'], null_probability = 0E0),
   orderstatus varchar(1) WITH (allowed_values = ARRAY['N','Q','H'], null_probability = 0E0),
   totalprice double WITH (max = '466001.28', min = '874.89', null_probability = 0E0),
   orderdate date WITH (max = '1998-08-02', min = '1992-01-01', null_probability = 0E0),
   orderpriority varchar(15) WITH (allowed_values = ARRAY['Maiores natu','Nihil cor','Quibusdam inci','Aut veni','Verita'], null_probability = 0E0),
   clerk varchar(15) WITH (allowed_values = ARRAY['Quos ab dolor','Excepturi nobis', ..., 'Explicabo la'], null_probability = 0E0),
   shippriority integer WITH (allowed_values = ARRAY['0'], null_probability = 0E0),
   comment varchar(79) WITH (null_probability = 0E0)
)
WITH (
   default_limit = 15000
)
```

### Related tables

To generate tables related by a foreign key, generate the primary key as a
sequence, by setting the `min` and `step` column properties. Limit the foreign
key column to the range of the primary key values, by setting its `min` and
`max`, or `allowed_values` column properties. Then every foreign key value
exists in the referenced table:

```sql
CREATE TABLE generator.default.authors (
  id INTEGER NOT NULL WITH (min = '1', step = '1'),
  name VARCHAR NOT NULL WITH (generator = '#{Book.author}')
)
WITH (default_limit = 100);

CREATE TABLE generator.default.books (
  id INTEGER NOT NULL WITH (min = '1', step = '1'),
  author_id INTEGER NOT NULL WITH (min = '1', max = '100'),
  title VARCHAR NOT NULL WITH (generator = '#{Book.title}')
)
WITH (default_limit = 1000);
```

The `authors` table has 100 rows, so its `id` values range from 1 to 100, the
same as the `author_id` values. When reading fewer rows from the `authors`
table, for example using `LIMIT`, some books reference authors that don't
exist.

When creating tables using existing data, Faker usually sets these properties
automatically for integer keys, because it detects sequences and uses the range
of the existing foreign key values. For example, after copying the `customer`
and `orders` tables from the TPC-H connector, every `custkey` value in
`orders` exists in `customer`. This doesn't work for keys of other types, like
`UUID` or `VARCHAR`, because Faker generates random values for them. For such
keys, set the [references](faker-references) column property instead.
