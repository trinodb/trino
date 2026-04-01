# Spark Variant Compatibility

This note records the current Spark/Iceberg compatibility story for `VARIANT` in this branch.

## What Trino Implements

Trino implements Iceberg `VARIANT` support in this branch.

On the Trino side, we currently verify Spark interop through product tests in:

- `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java`
- `testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/env/environment/EnvSinglenodeSparkIcebergRest.java`

Those tests currently cover:

- `Trino writes -> Spark reads` for `AVRO`
- `Trino writes -> Spark reads` for `PARQUET`
- `Spark writes -> Trino reads` for `AVRO`
- `Spark writes -> Trino reads` for `PARQUET`

Trino does not expose Iceberg Hadoop catalog as a first-class catalog type. The supported catalog types are defined in:

- `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/CatalogType.java`

There is no `HADOOP` entry there.

However, Trino can still interoperate with Hadoop-catalog Iceberg tables after registration by location. That path is
exercised in:

- `plugin/trino-iceberg/src/test/java/io/trino/plugin/iceberg/TestIcebergRegisterTableProcedure.java`

Specifically, `testRegisterHadoopTableAndRead()` shows Trino registering and reading a Hadoop-created Iceberg table.

## What Spark Implements

Spark itself has native `variant` support in Spark 4:

- [Spark `VariantType`](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/types/VariantType.scala)

The main compatibility limitations here are not about Spark SQL syntax. They are about:

- which Iceberg catalog path is being used
- which Iceberg REST server version is being used
- which file formats Spark Iceberg can actually read and write for `VARIANT`
- which Iceberg variant primitive encodings Spark's runtime understands

## Catalog Support

### Hive Metastore catalog

Spark `VARIANT` table creation through the Iceberg Hive Metastore path is not supported.

Reason:

- upstream Iceberg Hive schema conversion still has no `VARIANT` mapping and throws unsupported-type errors

Source:

- [Iceberg `HiveSchemaUtil.java`](https://github.com/apache/iceberg/blob/main/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveSchemaUtil.java)

This is an upstream Iceberg Hive-catalog limitation, not a Trino syntax issue.

### REST catalog

Spark `VARIANT` works with the Iceberg REST catalog only if the REST server is new enough.

Old REST server behavior:

- older Iceberg schema parsing used `Types.fromPrimitiveString(...)`
- that rejects `variant`

Source:

- [Iceberg 1.6.0 `SchemaParser.java`](https://github.com/apache/iceberg/blob/apache-iceberg-1.6.0/core/src/main/java/org/apache/iceberg/SchemaParser.java)

Newer REST server behavior:

- newer Iceberg schema parsing uses `Types.fromTypeName(...)`
- that accepts `variant`

Source:

- [Iceberg 1.10.1 `SchemaParser.java`](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/SchemaParser.java)

In our test environment, the original REST setup was on the older `tabulario/iceberg-rest:1.5.0` line, which is why
Spark table creation failed there. We now use a newer local REST image in the product test environment.

### Hadoop/storage-based catalog

Spark supports `VARIANT` with Iceberg's Hadoop catalog.

Here "storage-based catalog" means Iceberg's Hadoop catalog, where table metadata is stored directly in the warehouse
instead of going through Hive Metastore or REST.

Upstream evidence:

- Iceberg's own Spark variant test explicitly uses a Hadoop catalog to avoid Hive schema conversion

Source:

- [Iceberg `TestSparkVariantRead.java`](https://github.com/apache/iceberg/blob/main/spark/v4.0/spark/src/test/java/org/apache/iceberg/spark/sql/TestSparkVariantRead.java)

## File Format Support

### AVRO

This is implemented in Trino and currently works in our Spark compatibility tests.

### PARQUET

This is implemented in Trino and currently works in our Spark compatibility tests.

### ORC

Spark does not currently support the `VARIANT` ORC path well enough for us to include it in the compatibility matrix.

The important statement here is about Spark/Iceberg support, not Trino support:

- Trino implements Iceberg `VARIANT`
- Spark Iceberg ORC read/write support for `VARIANT` is the part that is not working

Why we consider it unsupported:

- the Spark Iceberg ORC reader/writer code paths do not contain variant-aware handling analogous to the variant-specific
  Spark tests
- in local product testing, Spark ORC variant interop failed in those code paths

Sources:

- [Iceberg `SparkOrcReader.java`](https://github.com/apache/iceberg/blob/main/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/data/SparkOrcReader.java)
- [Iceberg `SparkOrcWriter.java`](https://github.com/apache/iceberg/blob/main/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/data/SparkOrcWriter.java)

So the current state is:

- `AVRO`: yes
- `PARQUET`: yes
- `ORC`: no, because Spark/Iceberg ORC support for `VARIANT` is not there yet in a usable form for interop

## Variant Primitive Encodings

Trino follows the Iceberg variant encoding set here. The compatibility gap is that Spark 4's variant runtime only
understands a subset of the Iceberg variant primitive encodings.

### Currently verified in both directions

These values are currently covered by product tests for both `Trino writes -> Spark reads` and
`Spark writes -> Trino reads`:

- `null`
- variant `null`
- `boolean`
- `tinyint`
- `smallint`
- `integer`
- `bigint`
- `real`
- `double`
- `decimal`
- `varchar`
- `date`
- array values

### Currently verified only for `Trino writes -> Spark reads`

These values are already covered when Trino writes Iceberg `VARIANT` data and Spark reads it, but are not yet in the
verified Spark-written overlap:

- `varbinary`
- map/object values
- row/object values
- `uuid`

### Iceberg variant primitive encodings that Spark does not currently understand

These Iceberg variant primitive encodings are not currently understood by the Spark 4.0 variant runtime shipped in our
test image:

- `TIME_NTZ_MICROS`
- `TIMESTAMP_UTC_NANOS`
- `TIMESTAMP_NTZ_NANOS`

Why:

1. Iceberg 1.10.1 itself defines these physical types in its variant model:
    - [Iceberg `PhysicalType.java`](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/api/src/main/java/org/apache/iceberg/variants/PhysicalType.java)
    - [Iceberg `Primitives.java`](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/api/src/main/java/org/apache/iceberg/variants/Primitives.java)
2. Trino's variant header numbering matches those same physical encodings:
    - `core/trino-spi/src/main/java/io/trino/spi/variant/Header.java`
3. Spark's runtime `VariantUtil` only recognizes the primitive IDs it knows about and throws
   `UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT` for the unknown ones:
    - [Spark `VariantUtil.java`](https://github.com/apache/spark/blob/master/common/variant/src/main/java/org/apache/spark/types/variant/VariantUtil.java)

In the Spark 4.0.0 runtime shipped in our test image, the unsupported IDs line up with Trino's numbering exactly:

- `17` -> `TIME_NTZ_MICROS`
- `18` -> `TIMESTAMP_UTC_NANOS`
- `19` -> `TIMESTAMP_NTZ_NANOS`

This is why some valid Iceberg/Trino-written `VARIANT` values still cannot be read by Spark even when the catalog and
file format are otherwise supported.

## Summary

The short version is:

- Trino implements Iceberg `VARIANT`
- Spark also implements `variant`
- Hive Metastore catalog is still blocked upstream in Iceberg
- REST catalog works only with a new enough Iceberg REST server
- Hadoop/storage-based catalog works on the Spark side
- ORC is currently blocked by Spark/Iceberg support, not by Trino
- Spark still does not understand every Iceberg variant primitive encoding

## References

- [Iceberg `HiveSchemaUtil.java`](https://github.com/apache/iceberg/blob/main/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveSchemaUtil.java)
- [Iceberg 1.6.0 `SchemaParser.java`](https://github.com/apache/iceberg/blob/apache-iceberg-1.6.0/core/src/main/java/org/apache/iceberg/SchemaParser.java)
- [Iceberg 1.10.1 `SchemaParser.java`](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/SchemaParser.java)
- [Iceberg `TestSparkVariantRead.java`](https://github.com/apache/iceberg/blob/main/spark/v4.0/spark/src/test/java/org/apache/iceberg/spark/sql/TestSparkVariantRead.java)
- [Iceberg `SparkOrcReader.java`](https://github.com/apache/iceberg/blob/main/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/data/SparkOrcReader.java)
- [Iceberg `SparkOrcWriter.java`](https://github.com/apache/iceberg/blob/main/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/data/SparkOrcWriter.java)
- [Iceberg `PhysicalType.java`](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/api/src/main/java/org/apache/iceberg/variants/PhysicalType.java)
- [Iceberg `Primitives.java`](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/api/src/main/java/org/apache/iceberg/variants/Primitives.java)
- [Spark `VariantUtil.java`](https://github.com/apache/spark/blob/master/common/variant/src/main/java/org/apache/spark/types/variant/VariantUtil.java)
- [Spark `VariantType.scala`](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/types/VariantType.scala)
- `core/trino-spi/src/main/java/io/trino/spi/variant/Header.java`
- `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/CatalogType.java`
- `plugin/trino-iceberg/src/test/java/io/trino/plugin/iceberg/TestIcebergRegisterTableProcedure.java`
- `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java`
