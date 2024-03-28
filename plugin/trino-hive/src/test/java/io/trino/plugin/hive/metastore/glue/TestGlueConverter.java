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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.TableType;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.security.PrincipalType;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.metastore.glue.GlueConverter.PUBLIC_OWNER;
import static io.trino.plugin.hive.util.HiveUtil.DELTA_LAKE_PROVIDER;
import static io.trino.plugin.hive.util.HiveUtil.ICEBERG_TABLE_TYPE_NAME;
import static io.trino.plugin.hive.util.HiveUtil.ICEBERG_TABLE_TYPE_VALUE;
import static io.trino.plugin.hive.util.HiveUtil.SPARK_TABLE_PROVIDER_KEY;
import static org.assertj.core.api.Assertions.assertThat;

class TestGlueConverter
{
    private final Database trinoDatabase = Database.builder()
            .setDatabaseName("test-database")
            .setComment(Optional.of("database desc"))
            .setLocation(Optional.of("/database"))
            .setParameters(ImmutableMap.of())
            .setOwnerName(Optional.of("PUBLIC"))
            .setOwnerType(Optional.of(PrincipalType.ROLE))
            .build();

    private final Table trinoTable = Table.builder()
            .setDatabaseName(trinoDatabase.getDatabaseName())
            .setTableName("test-table")
            .setOwner(Optional.of("owner"))
            .setParameters(ImmutableMap.of())
            .setTableType(EXTERNAL_TABLE.name())
            .setDataColumns(ImmutableList.of(new Column("table-data", HIVE_STRING, Optional.of("table data column comment"), Map.of())))
            .setPartitionColumns(ImmutableList.of(new Column("table-partition", HIVE_STRING, Optional.of("table partition column comment"), Map.of())))
            .setViewOriginalText(Optional.of("originalText"))
            .setViewExpandedText(Optional.of("expandedText"))
            .withStorage(storage -> storage.setStorageFormat(StorageFormat.create("TableSerdeLib", "TableInputFormat", "TableOutputFormat"))
                    .setLocation("/test-table")
                    .setBucketProperty(Optional.empty())
                    .setSerdeParameters(ImmutableMap.of())).build();

    private final Partition trinoPartition = Partition.builder()
            .setDatabaseName(trinoDatabase.getDatabaseName())
            .setTableName(trinoTable.getTableName())
            .setValues(ImmutableList.of("val1"))
            .setColumns(ImmutableList.of(new Column("partition-data", HIVE_STRING, Optional.of("partition data column comment"), Map.of())))
            .setParameters(ImmutableMap.of())
            .withStorage(storage -> storage.setStorageFormat(StorageFormat.create("PartitionSerdeLib", "PartitionInputFormat", "PartitionOutputFormat"))
                    .setLocation("/test-table/partition")
                    .setBucketProperty(Optional.empty())
                    .setSerdeParameters(ImmutableMap.of())).build();

    private final software.amazon.awssdk.services.glue.model.Database glueDatabase = software.amazon.awssdk.services.glue.model.Database.builder()
            .name("test-database")
            .description("database desc")
            .locationUri("/database")
            .parameters(ImmutableMap.of("key", "database-value"))
            .build();

    private final software.amazon.awssdk.services.glue.model.Table glueMaterializedView = software.amazon.awssdk.services.glue.model.Table.builder()
            .databaseName(glueDatabase.name())
            .name("test-materialized-view")
            .owner("owner")
            .parameters(ImmutableMap.<String, String>builder()
                    .put(PRESTO_VIEW_FLAG, "true")
                    .put(TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT)
                    .buildOrThrow())
            .tableType(TableType.VIRTUAL_VIEW.name())
            .viewOriginalText("/* %s: base64encodedquery */".formatted(ICEBERG_MATERIALIZED_VIEW_COMMENT))
            .viewExpandedText(ICEBERG_MATERIALIZED_VIEW_COMMENT)
            .build();

    private final software.amazon.awssdk.services.glue.model.Table glueTable = software.amazon.awssdk.services.glue.model.Table.builder()
            .databaseName(glueDatabase.name())
            .name("test-table")
            .owner("owner")
            .parameters(ImmutableMap.of())
            .partitionKeys(software.amazon.awssdk.services.glue.model.Column.builder()
                    .name("table-partition")
                    .type("string")
                    .comment("table partition column comment")
                    .build())
            .storageDescriptor(software.amazon.awssdk.services.glue.model.StorageDescriptor.builder()
                    .bucketColumns(ImmutableList.of("test-bucket-col"))
                    .columns(software.amazon.awssdk.services.glue.model.Column.builder()
                            .name("table-data")
                            .type("string")
                            .comment("table data column comment")
                            .build())
                    .parameters(ImmutableMap.of())
                    .serdeInfo(SerDeInfo.builder()
                            .serializationLibrary("SerdeLib")
                            .parameters(ImmutableMap.of())
                            .build())
                    .inputFormat("InputFormat")
                    .outputFormat("OutputFormat")
                    .location("/test-table")
                    .numberOfBuckets(1)
                    .build())
            .tableType(EXTERNAL_TABLE.name())
            .viewOriginalText("originalText")
            .viewExpandedText("expandedText")
            .build();

    private final software.amazon.awssdk.services.glue.model.Partition gluePartition = software.amazon.awssdk.services.glue.model.Partition.builder()
            .databaseName(glueDatabase.name())
            .tableName(glueTable.name())
            .values(ImmutableList.of("val1"))
            .parameters(ImmutableMap.of())
            .storageDescriptor(software.amazon.awssdk.services.glue.model.StorageDescriptor.builder()
                    .bucketColumns(ImmutableList.of("partition-bucket-col"))
                    .columns(software.amazon.awssdk.services.glue.model.Column.builder()
                            .name("partition-data")
                            .type("string")
                            .comment("partition data column comment")
                            .build())
                    .parameters(ImmutableMap.of())
                    .serdeInfo(SerDeInfo.builder()
                            .serializationLibrary("SerdeLib")
                            .parameters(ImmutableMap.of())
                            .build())
                    .inputFormat("InputFormat")
                    .outputFormat("OutputFormat")
                    .location("/test-table")
                    .numberOfBuckets(1)
                    .build())
            .build();

    @Test
    void testToGlueDatabaseInput()
    {
        DatabaseInput databaseInput = GlueConverter.toGlueDatabaseInput(trinoDatabase);

        assertThat(databaseInput.name()).isEqualTo(trinoDatabase.getDatabaseName());
        assertThat(databaseInput.description()).isEqualTo(trinoDatabase.getComment().orElse(null));
        assertThat(databaseInput.locationUri()).isEqualTo(trinoDatabase.getLocation().orElse(null));
        assertThat(databaseInput.parameters()).isEqualTo(trinoDatabase.getParameters());
    }

    @Test
    void testToGlueTableInput()
    {
        TableInput tableInput = GlueConverter.toGlueTableInput(trinoTable);

        assertThat(tableInput.name()).isEqualTo(trinoTable.getTableName());
        assertThat(tableInput.owner()).isEqualTo(trinoTable.getOwner().orElse(null));
        assertThat(tableInput.tableType()).isEqualTo(trinoTable.getTableType());
        assertThat(tableInput.parameters()).isEqualTo(trinoTable.getParameters());
        assertColumnList(tableInput.storageDescriptor().columns(), trinoTable.getDataColumns());
        assertColumnList(tableInput.partitionKeys(), trinoTable.getPartitionColumns());
        assertStorage(tableInput.storageDescriptor(), trinoTable.getStorage());
        assertThat(tableInput.viewExpandedText()).isEqualTo(trinoTable.getViewExpandedText().orElse(null));
        assertThat(tableInput.viewOriginalText()).isEqualTo(trinoTable.getViewOriginalText().orElse(null));
    }

    @Test
    void testToGluePartitionInput()
    {
        PartitionInput partitionInput = GlueConverter.toGluePartitionInput(trinoPartition);

        assertThat(partitionInput.parameters()).isEqualTo(trinoPartition.getParameters());
        assertStorage(partitionInput.storageDescriptor(), trinoPartition.getStorage());
        assertThat(partitionInput.values()).isEqualTo(trinoPartition.getValues());
    }

    @Test
    void testToGlueFunctionInput()
    {
        // random data to avoid compression, but deterministic for size assertion
        String sql = HexFormat.of().formatHex(Slices.random(2000, new Random(0)).getBytes());
        LanguageFunction expected = new LanguageFunction("(integer,bigint,varchar)", sql, List.of(), Optional.of("owner"));

        UserDefinedFunctionInput input = GlueConverter.toGlueFunctionInput("test_name", expected);
        assertThat(input.ownerName()).isEqualTo(expected.owner().orElseThrow());

        UserDefinedFunction function = UserDefinedFunction.builder()
                .ownerName(input.ownerName())
                .resourceUris(input.resourceUris())
                .build();
        LanguageFunction actual = GlueConverter.fromGlueFunction(function);

        assertThat(input.resourceUris().size()).isEqualTo(4);
        assertThat(actual).isEqualTo(expected);

        // verify that the owner comes from the metastore
        function = function.toBuilder().ownerName("other").build();
        actual = GlueConverter.fromGlueFunction(function);
        assertThat(actual.owner()).isEqualTo(Optional.of("other"));
    }

    @Test
    void testConvertDatabase()
    {
        io.trino.plugin.hive.metastore.Database trinoDatabase = GlueConverter.fromGlueDatabase(glueDatabase);
        assertThat(trinoDatabase.getDatabaseName()).isEqualTo(glueDatabase.name());
        assertThat(trinoDatabase.getLocation()).hasValue(glueDatabase.locationUri());
        assertThat(trinoDatabase.getComment()).hasValue(glueDatabase.description());
        assertThat(trinoDatabase.getParameters()).isEqualTo(glueDatabase.parameters());
        assertThat(trinoDatabase.getOwnerName()).isEqualTo(Optional.of(PUBLIC_OWNER));
        assertThat(trinoDatabase.getOwnerType()).isEqualTo(Optional.of(PrincipalType.ROLE));
    }

    @Test
    void testConvertTable()
    {
        io.trino.plugin.hive.metastore.Table trinoTable = GlueConverter.fromGlueTable(glueTable, glueDatabase.name());
        assertThat(trinoTable.getTableName()).isEqualTo(glueTable.name());
        assertThat(trinoTable.getDatabaseName()).isEqualTo(glueDatabase.name());
        assertThat(trinoTable.getTableType()).isEqualTo(glueTable.tableType());
        assertThat(trinoTable.getOwner().orElse(null)).isEqualTo(glueTable.owner());
        assertThat(trinoTable.getParameters()).isEqualTo(glueTable.parameters());
        assertColumnList(glueTable.storageDescriptor().columns(), trinoTable.getDataColumns());
        assertColumnList(glueTable.partitionKeys(), trinoTable.getPartitionColumns());
        assertStorage(glueTable.storageDescriptor(), trinoTable.getStorage());
        assertThat(trinoTable.getViewOriginalText()).hasValue(glueTable.viewOriginalText());
        assertThat(trinoTable.getViewExpandedText()).hasValue(glueTable.viewExpandedText());
    }

    @Test
    void testConvertTableWithOpenCSVSerDe()
    {
        software.amazon.awssdk.services.glue.model.Table glueTable = this.glueTable.toBuilder()
                .storageDescriptor(this.glueTable.storageDescriptor().toBuilder()
                        .columns(ImmutableList.of(software.amazon.awssdk.services.glue.model.Column.builder()
                                .name("int_column")
                                .type("int")
                                .comment("int column")
                                .build()))
                        .inputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
                        .outputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
                        .serdeInfo(SerDeInfo.builder()
                                .serializationLibrary("org.apache.hadoop.hive.serde2.OpenCSVSerde")
                                .parameters(ImmutableMap.of())
                                .build())
                        .build())
                .build();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueConverter.fromGlueTable(glueTable, glueTable.databaseName());

        assertThat(trinoTable.getTableName()).isEqualTo(glueTable.name());
        assertThat(trinoTable.getDatabaseName()).isEqualTo(glueDatabase.name());
        assertThat(trinoTable.getTableType()).isEqualTo(glueTable.tableType());
        assertThat(trinoTable.getOwner().orElse(null)).isEqualTo(glueTable.owner());
        assertThat(trinoTable.getParameters()).isEqualTo(glueTable.parameters());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
        assertThat(trinoTable.getDataColumns().getFirst().getType()).isEqualTo(HIVE_STRING);

        assertColumnList(glueTable.partitionKeys(), trinoTable.getPartitionColumns());
        assertStorage(glueTable.storageDescriptor(), trinoTable.getStorage());
        assertThat(trinoTable.getViewOriginalText()).hasValue(glueTable.viewOriginalText());
        assertThat(trinoTable.getViewExpandedText()).hasValue(glueTable.viewExpandedText());
    }

    @Test
    void testConvertTableWithoutTableType()
    {
        software.amazon.awssdk.services.glue.model.Table table = glueTable.toBuilder()
                .tableType(null)
                .build();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueConverter.fromGlueTable(table, table.databaseName());
        assertThat(trinoTable.getTableType()).isEqualTo(EXTERNAL_TABLE.name());
    }

    @Test
    void testConvertTableNullPartitions()
    {
        software.amazon.awssdk.services.glue.model.Table table = glueTable.toBuilder()
                .partitionKeys(ImmutableList.of())
                .build();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueConverter.fromGlueTable(table, table.databaseName());
        assertThat(trinoTable.getPartitionColumns().isEmpty()).isTrue();
    }

    @Test
    void testConvertTableUppercaseColumnType()
    {
        software.amazon.awssdk.services.glue.model.Table table = glueTable.toBuilder()
                .partitionKeys(software.amazon.awssdk.services.glue.model.Column.builder()
                        .name("table-partition")
                        .type("String")
                        .comment("table partition column comment")
                        .build())
                .build();

        assertThat(GlueConverter.fromGlueTable(table, table.databaseName()).getPartitionColumns().getFirst().getType()).isEqualTo(HIVE_STRING);
    }

    @Test
    void testConvertPartition()
    {
        io.trino.plugin.hive.metastore.Partition trinoPartition = GlueConverter.fromGluePartition(gluePartition.databaseName(), gluePartition.tableName(), gluePartition);
        assertThat(trinoPartition.getDatabaseName()).isEqualTo(gluePartition.databaseName());
        assertThat(trinoPartition.getTableName()).isEqualTo(gluePartition.tableName());
        assertColumnList(gluePartition.storageDescriptor().columns(), trinoPartition.getColumns());
        assertThat(trinoPartition.getValues()).isEqualTo(gluePartition.values());
        assertStorage(gluePartition.storageDescriptor(), trinoPartition.getStorage());
        assertThat(trinoPartition.getParameters()).isEqualTo(gluePartition.parameters());
    }

    @Test
    void testDatabaseNullParameters()
    {
        software.amazon.awssdk.services.glue.model.Database database = glueDatabase.toBuilder()
                .parameters(null)
                .build();
        assertThat(GlueConverter.fromGlueDatabase(database).getParameters()).isNotNull();
    }

    @Test
    void testTableNullParameters()
    {
        software.amazon.awssdk.services.glue.model.Table table = glueTable.toBuilder()
                .parameters(null)
                .storageDescriptor(glueTable.storageDescriptor().toBuilder()
                        .serdeInfo(glueTable.storageDescriptor().serdeInfo().toBuilder()
                                .parameters(null)
                                .build())
                        .build())
                .build();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueConverter.fromGlueTable(table, glueTable.databaseName());
        assertThat(trinoTable.getParameters()).isNotNull();
        assertThat(trinoTable.getStorage().getSerdeParameters()).isNotNull();
    }

    @Test
    void testIcebergTableNullStorageDescriptor()
    {
        software.amazon.awssdk.services.glue.model.Table table = glueTable.toBuilder()
                .parameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE))
                .storageDescriptor((StorageDescriptor) null)
                .build();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueConverter.fromGlueTable(table, table.databaseName());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
    }

    @Test
    void testIcebergTableNonNullStorageDescriptor()
    {
        software.amazon.awssdk.services.glue.model.Table table = glueTable.toBuilder()
                .parameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE))
                .build();
        assertThat(table.storageDescriptor()).isNotNull();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueConverter.fromGlueTable(table, table.databaseName());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
    }

    @Test
    void testDeltaTableNullStorageDescriptor()
    {
        software.amazon.awssdk.services.glue.model.Table table = glueTable.toBuilder()
                .parameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER))
                .storageDescriptor((StorageDescriptor) null)
                .build();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueConverter.fromGlueTable(table, table.databaseName());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
    }

    @Test
    void testDeltaTableNonNullStorageDescriptor()
    {
        software.amazon.awssdk.services.glue.model.Table table = glueTable.toBuilder()
                .parameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER))
                .build();
        assertThat(table.storageDescriptor()).isNotNull();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueConverter.fromGlueTable(table, table.databaseName());
        assertThat(trinoTable.getDataColumns().stream()
                .map(Column::getName)
                .collect(toImmutableSet())).isEqualTo(glueTable.storageDescriptor().columns().stream()
                .map(software.amazon.awssdk.services.glue.model.Column::name)
                .collect(toImmutableSet()));
    }

    @Test
    public void testIcebergMaterializedViewNullStorageDescriptor()
    {
        assertThat(glueMaterializedView.storageDescriptor()).isNull();
        Table trinoTable = GlueConverter.fromGlueTable(glueMaterializedView, glueMaterializedView.databaseName());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
    }

    @Test
    void testPartitionNullParameters()
    {
        software.amazon.awssdk.services.glue.model.Partition partition = gluePartition.toBuilder()
                .parameters(null)
                .build();
        assertThat(GlueConverter.fromGluePartition(partition.databaseName(), partition.tableName(), partition).getParameters()).isNotNull();
    }

    private static void assertColumnList(List<software.amazon.awssdk.services.glue.model.Column> glueColumns, List<Column> trinoColumns)
    {
        if (trinoColumns == null) {
            assertThat(glueColumns).isNull();
        }
        assertThat(glueColumns).isNotNull();
        assertThat(glueColumns.size()).isEqualTo(trinoColumns.size());

        for (int i = 0; i < trinoColumns.size(); i++) {
            assertColumn(glueColumns.get(i), trinoColumns.get(i));
        }
    }

    private static void assertColumn(software.amazon.awssdk.services.glue.model.Column glueColumn, Column trinoColumn)
    {
        assertThat(glueColumn.name()).isEqualTo(trinoColumn.getName());
        assertThat(glueColumn.type()).isEqualTo(trinoColumn.getType().getHiveTypeName().toString());
        assertThat(glueColumn.comment()).isEqualTo(trinoColumn.getComment().orElse(null));
    }

    private static void assertStorage(StorageDescriptor glueStorage, Storage trinoStorage)
    {
        assertThat(glueStorage.location()).isEqualTo(trinoStorage.getLocation());
        assertThat(glueStorage.serdeInfo().serializationLibrary()).isEqualTo(trinoStorage.getStorageFormat().getSerde());
        assertThat(glueStorage.inputFormat()).isEqualTo(trinoStorage.getStorageFormat().getInputFormat());
        assertThat(glueStorage.outputFormat()).isEqualTo(trinoStorage.getStorageFormat().getOutputFormat());

        if (trinoStorage.getBucketProperty().isPresent()) {
            HiveBucketProperty bucketProperty = trinoStorage.getBucketProperty().get();
            assertThat(glueStorage.bucketColumns()).isEqualTo(bucketProperty.getBucketedBy());
            assertThat(glueStorage.numberOfBuckets().intValue()).isEqualTo(bucketProperty.getBucketCount());
        }
    }
}
