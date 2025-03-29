/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.catalog.jdbc.IcebergJdbcCatalogConfig;
import io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantTypeTestUtils;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.stream.Stream;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.VARIANT;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.PASSWORD;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.USER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.hadoop.hive.ql.io.IOConstants.PARQUET;
import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;
import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.jdbc.JdbcCatalog.PROPERTY_PREFIX;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergVariantParquet
        extends AbstractTestQueryFramework
{
    private JdbcCatalog jdbcCatalog;
    private File warehouseLocation;

    private static final String INT_COL_NAME = "id";
    private static final String VARIANT_COL_NAME = "var";
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, INT_COL_NAME, Types.IntegerType.get()),
            Types.NestedField.required(2, VARIANT_COL_NAME, Types.VariantType.get()));
    private static final ColumnIdentity INT_COLUMN_IDENTITY = new ColumnIdentity(1, INT_COL_NAME, PRIMITIVE, ImmutableList.of());
    private static final ColumnIdentity VARIANT_COLUMN_IDENTITY = new ColumnIdentity(2, VARIANT_COL_NAME, VARIANT, ImmutableList.of());
    private static final GenericRecord RECORD = GenericRecord.create(SCHEMA);

    private static final ByteBuffer TEST_METADATA_BUFFER = VariantTypeTestUtils.createMetadata(ImmutableList.of("a", "b", "c", "d", "e"), true);
    private static final ByteBuffer TEST_OBJECT_BUFFER = VariantTypeTestUtils.createObject(
            TEST_METADATA_BUFFER,
            ImmutableMap.of(
                    "a", Variants.ofNull(),
                    "d", Variants.of("trino")));
    private static final ByteBuffer SIMILAR_OBJECT_BUFFER = VariantTypeTestUtils.createObject(
            TEST_METADATA_BUFFER,
            ImmutableMap.of(
                    "a", Variants.of(123456789),
                    "c", Variants.of("string")));
    private static final ByteBuffer EMPTY_OBJECT_BUFFER = VariantTypeTestUtils.createObject(TEST_METADATA_BUFFER, ImmutableMap.of());

    private static final VariantMetadata TEST_METADATA = Variants.metadata(TEST_METADATA_BUFFER);
    private static final VariantMetadata EMPTY_METADATA = Variants.metadata(VariantTypeTestUtils.emptyMetadata());
    private static final VariantObject TEST_OBJECT = (VariantObject) Variants.value(TEST_METADATA, TEST_OBJECT_BUFFER);
    private static final VariantObject SIMILAR_OBJECT = (VariantObject) Variants.value(TEST_METADATA, SIMILAR_OBJECT_BUFFER);
    private static final VariantObject EMPTY_OBJECT = (VariantObject) Variants.value(TEST_METADATA, EMPTY_OBJECT_BUFFER);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouseLocation = Files.createTempDirectory("test_iceberg_variant_test").toFile();
        closeAfterClass(() -> deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE));
        TestingIcebergJdbcServer server = closeAfterClass(new TestingIcebergJdbcServer());
        jdbcCatalog = (JdbcCatalog) buildIcebergCatalog("tpch", ImmutableMap.<String, String>builder()
                        .put(CATALOG_IMPL, JdbcCatalog.class.getName())
                        .put(URI, server.getJdbcUrl())
                        .put(PROPERTY_PREFIX + "user", USER)
                        .put(PROPERTY_PREFIX + "password", PASSWORD)
                        .put(PROPERTY_PREFIX + "schema-version", IcebergJdbcCatalogConfig.SchemaVersion.V1.toString())
                        .put(WAREHOUSE_LOCATION, warehouseLocation.getAbsolutePath())
                        .buildOrThrow(),
                new Configuration(false));
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", PARQUET)
                                .put("iceberg.catalog.type", "jdbc")
                                .put("iceberg.jdbc-catalog.driver-class", "org.postgresql.Driver")
                                .put("iceberg.jdbc-catalog.connection-url", server.getJdbcUrl())
                                .put("iceberg.jdbc-catalog.connection-user", USER)
                                .put("iceberg.jdbc-catalog.connection-password", PASSWORD)
                                .put("iceberg.jdbc-catalog.catalog-name", "tpch")
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .put("iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max")
                                .put("iceberg.jdbc-catalog.default-warehouse-dir", warehouseLocation.getAbsolutePath())
                                .put("iceberg.jdbc-catalog.retryable-status-codes", "57P01,57P05")
                                .buildOrThrow())
                .build();
    }

    @ParameterizedTest
    @MethodSource("variantTypeResultMappings")
    public void testVariantTypeMappings(Variant variant, String expected)
            throws Exception
    {
        TableIdentifier tableIdentifier = TableIdentifier.of("tpch", "variant_" + randomNameSuffix());
        Table table = jdbcCatalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned(), null, ImmutableMap.of("format-version", "3"));

        OutputFile outputFile = localOutput(warehouseLocation + "/variant-%s.parquet".formatted(randomNameSuffix()));
        Record record = RECORD.copy("id", 1, "var", variant);

        try (FileAppender<Record> writer = Parquet.write(outputFile)
                .schema(SCHEMA)
                .variantShreddingFunc((_, _) -> null)
                .createWriterFunc(fileSchema -> InternalWriter.create(SCHEMA.asStruct(), fileSchema))
                .build()) {
            writer.add(record);
            DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                    .withRecordCount(1)
                    .withFileSizeInBytes(2000)
                    .withPath(outputFile.location())
                    .withFormat(FileFormat.PARQUET)
                    .build();

            table.newAppend().appendFile(file).commit();
        }
        assertQuery("SELECT * FROM " + tableIdentifier.name(), expected);
        assertUpdate("DROP TABLE IF EXISTS " + tableIdentifier.name());
    }

    private static Stream<Arguments> variantTypeResultMappings()
    {
        return Stream.of(
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.ofNull()), "VALUES (1, 'null')"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(true)), "VALUES (1, 'true')"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(false)), "VALUES (1, 'false')"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of((byte) 34)), "VALUES (1, 34)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of((byte) -34)), "VALUES (1, -34)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of((short) 1234)), "VALUES (1, 1234)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of((short) -1234)), "VALUES (1, -1234)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(12345)), "VALUES (1, 12345)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(-12345)), "VALUES (1, -12345)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(9876543210L)), "VALUES (1, 9876543210L)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(-9876543210L)), "VALUES (1, -9876543210L)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(10.11F)), "VALUES (1, 10.11)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(-10.11F)), "VALUES (1, -10.11)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(14.3D)), "VALUES (1, 14.3)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(-14.3D)), "VALUES (1, -14.3)"),
                Arguments.of(Variant.of(EMPTY_METADATA, EMPTY_OBJECT), "VALUES (1, JSON '{}')"),
                Arguments.of(Variant.of(TEST_METADATA, TEST_OBJECT), "VALUES (1, JSON '{\"a\":null,\"d\":\"trino\"}')"),
                Arguments.of(Variant.of(TEST_METADATA, SIMILAR_OBJECT), "VALUES (1, JSON '{\"a\":123456789,\"c\":\"string\"}')"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.ofIsoDate("2024-11-07")), "VALUES (1, '\"2024-11-07\"')"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.ofIsoDate("1957-11-07")), "VALUES (1, '\"1957-11-07\"')"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00")), "VALUES (1, '\"2024-11-07 12:33:54.123456+00:00\"')"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.ofIsoTimestamptz("1957-11-07T12:33:54.123456+00:00")), "VALUES (1, '\"1957-11-07 12:33:54.123456+00:00\"')"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.ofIsoTimestampntz("2024-11-07T12:33:54.123456")), "VALUES (1, '\"2024-11-07 12:33:54.123456\"')"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.ofIsoTimestampntz("1957-11-07T12:33:54.123456")), "VALUES (1, '\"1957-11-07 12:33:54.123456\"')"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(new BigDecimal("123456.789"))), "VALUES (1, 123456.789)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(new BigDecimal("-123456.789"))), "VALUES (1, -123456.789)"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d}))), "VALUES (1, '\"CgsMDQ==\"')"),
                Arguments.of(Variant.of(EMPTY_METADATA, Variants.of("trino")), "VALUES (1, '\"trino\"')"));
    }

    @Test
    public void testVariantTypePartitionFails()
            throws Exception
    {
        TableIdentifier tableIdentifier = TableIdentifier.of("tpch", "variant_partition_spec" + randomNameSuffix());
        assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA)
                .identity(VARIANT_COLUMN_IDENTITY.getName())
                .build())
                .hasMessageContaining("Cannot partition by non-primitive source field: variant");

        PartitionSpec validPartitionSpec = PartitionSpec.builderFor(SCHEMA)
                .identity(INT_COLUMN_IDENTITY.getName())
                .build();
        OutputFile outputFile = localOutput(warehouseLocation + "/variant-part-%s.parquet".formatted(randomNameSuffix()));
        Table table = jdbcCatalog.createTable(tableIdentifier, SCHEMA, validPartitionSpec, null, ImmutableMap.of("format-version", "3"));
        Record record = RECORD.copy("id", 1, "var", Variant.of(TEST_METADATA, TEST_OBJECT));

        try (FileAppender<Record> writer = Parquet.write(outputFile)
                .schema(SCHEMA)
                .variantShreddingFunc((_, _) -> null)
                .createWriterFunc(fileSchema -> InternalWriter.create(SCHEMA.asStruct(), fileSchema))
                .build()) {
            writer.add(record);
            DataFile file = DataFiles.builder(validPartitionSpec)
                    .withPartition(PartitionData.fromJson("{\"partitionValues\":[\"1\"]}", new Type[] {Types.IntegerType.get()}))
                    .withRecordCount(1)
                    .withFileSizeInBytes(2000)
                    .withPath(outputFile.location())
                    .withFormat(FileFormat.PARQUET)
                    .build();

            table.newAppend().appendFile(file).commit();
        }
        assertQuery("SELECT * FROM " + tableIdentifier.name(), "VALUES (1, JSON '{\"a\":null,\"d\":\"trino\"}')");
        assertUpdate("DROP TABLE IF EXISTS " + tableIdentifier.name());
    }

    @Test
    public void testVariantRegisterTable()
            throws IOException
    {
        TableIdentifier tableIdentifier = TableIdentifier.of("tpch", "variant_" + randomNameSuffix());
        String registeredTableName = "registered_table_" + randomNameSuffix();
        BaseTable table = (BaseTable) jdbcCatalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned(), null, ImmutableMap.of("format-version", "3"));

        OutputFile outputFile = localOutput(table.operations().current().location() + "/data/variant-reg-%s.parquet".formatted(randomNameSuffix()));
        Record record = RECORD.copy("id", 1, "var", Variant.of(TEST_METADATA, TEST_OBJECT));

        try (FileAppender<Record> writer = Parquet.write(outputFile)
                .schema(SCHEMA)
                .variantShreddingFunc((_, _) -> null)
                .createWriterFunc(fileSchema -> InternalWriter.create(SCHEMA.asStruct(), fileSchema))
                .build()) {
            writer.add(record);
            DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                    .withRecordCount(1)
                    .withFileSizeInBytes(2000)
                    .withPath(outputFile.location())
                    .withFormat(FileFormat.PARQUET)
                    .build();

            table.newAppend().appendFile(file).commit();
            assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(registeredTableName, table.operations().current().location()));
        }
        assertQuery("SELECT * FROM " + registeredTableName, "VALUES (1, JSON '{\"a\":null,\"d\":\"trino\"}')");
        assertUpdate("DROP TABLE IF EXISTS " + tableIdentifier.name());
    }
}
