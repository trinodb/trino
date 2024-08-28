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
package io.trino.plugin.hive.metastore.glue.v2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Column;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.Storage;
import io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter;
import io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter.GluePartitionConverter;
import io.trino.spi.security.PrincipalType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.metastore.glue.v2.TestingMetastoreObjects.getGlueTestColumn;
import static io.trino.plugin.hive.metastore.glue.v2.TestingMetastoreObjects.getGlueTestDatabase;
import static io.trino.plugin.hive.metastore.glue.v2.TestingMetastoreObjects.getGlueTestPartition;
import static io.trino.plugin.hive.metastore.glue.v2.TestingMetastoreObjects.getGlueTestStorageDescriptor;
import static io.trino.plugin.hive.metastore.glue.v2.TestingMetastoreObjects.getGlueTestTable;
import static io.trino.plugin.hive.metastore.glue.v2.TestingMetastoreObjects.getGlueTestTrinoMaterializedView;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter.getPartitionParameters;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter.getTableParameters;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter.getTableTypeNullable;
import static io.trino.plugin.hive.util.HiveUtil.DELTA_LAKE_PROVIDER;
import static io.trino.plugin.hive.util.HiveUtil.ICEBERG_TABLE_TYPE_NAME;
import static io.trino.plugin.hive.util.HiveUtil.ICEBERG_TABLE_TYPE_VALUE;
import static io.trino.plugin.hive.util.HiveUtil.SPARK_TABLE_PROVIDER_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestGlueToTrinoConverter
{
    private static final String PUBLIC_OWNER = "PUBLIC";

    private Database testDatabase;
    private Table testTable;
    private Partition testPartition;

    @BeforeEach
    public void setup()
    {
        testDatabase = getGlueTestDatabase();
        testTable = getGlueTestTable(testDatabase.name());
        testPartition = getGlueTestPartition(testDatabase.name(), testTable.name(), ImmutableList.of("val1"));
    }

    private static GluePartitionConverter createPartitionConverter(Table table)
    {
        return new GluePartitionConverter(table.databaseName(), table.name());
    }

    @Test
    public void testConvertDatabase()
    {
        io.trino.metastore.Database trinoDatabase = GlueToTrinoConverter.convertDatabase(testDatabase);
        assertThat(trinoDatabase.getDatabaseName()).isEqualTo(testDatabase.name());
        assertThat(trinoDatabase.getLocation().get()).isEqualTo(testDatabase.locationUri());
        assertThat(trinoDatabase.getComment().get()).isEqualTo(testDatabase.description());
        assertThat(trinoDatabase.getParameters()).isEqualTo(testDatabase.parameters());
        assertThat(trinoDatabase.getOwnerName()).isEqualTo(Optional.of(PUBLIC_OWNER));
        assertThat(trinoDatabase.getOwnerType()).isEqualTo(Optional.of(PrincipalType.ROLE));
    }

    @Test
    public void testConvertTable()
    {
        io.trino.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testTable, testDatabase.name());
        assertThat(trinoTable.getTableName()).isEqualTo(testTable.name());
        assertThat(trinoTable.getDatabaseName()).isEqualTo(testDatabase.name());
        assertThat(trinoTable.getTableType()).isEqualTo(getTableTypeNullable(testTable));
        assertThat(trinoTable.getOwner().orElse(null)).isEqualTo(testTable.owner());
        assertThat(trinoTable.getParameters()).isEqualTo(getTableParameters(testTable));
        assertColumnList(trinoTable.getDataColumns(), testTable.storageDescriptor().columns());
        assertColumnList(trinoTable.getPartitionColumns(), testTable.partitionKeys());
        assertStorage(trinoTable.getStorage(), testTable.storageDescriptor());
        assertThat(trinoTable.getViewOriginalText().get()).isEqualTo(testTable.viewOriginalText());
        assertThat(trinoTable.getViewExpandedText().get()).isEqualTo(testTable.viewExpandedText());
    }

    @Test
    public void testConvertTableWithOpenCSVSerDe()
    {
        Table glueTable = getGlueTestTable(testDatabase.name())
                .toBuilder()
                .storageDescriptor(getGlueTestStorageDescriptor(
                ImmutableList.of(getGlueTestColumn("int")),
                "org.apache.hadoop.hive.serde2.OpenCSVSerde"))
                .build();
        io.trino.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(glueTable, testDatabase.name());

        assertThat(trinoTable.getTableName()).isEqualTo(glueTable.name());
        assertThat(trinoTable.getDatabaseName()).isEqualTo(testDatabase.name());
        assertThat(trinoTable.getTableType()).isEqualTo(getTableTypeNullable(glueTable));
        assertThat(trinoTable.getOwner().orElse(null)).isEqualTo(glueTable.owner());
        assertThat(trinoTable.getParameters()).isEqualTo(getTableParameters(glueTable));
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
        assertThat(trinoTable.getDataColumns().get(0).getType()).isEqualTo(HIVE_STRING);

        assertColumnList(trinoTable.getPartitionColumns(), glueTable.partitionKeys());
        assertStorage(trinoTable.getStorage(), glueTable.storageDescriptor());
        assertThat(trinoTable.getViewOriginalText().get()).isEqualTo(glueTable.viewOriginalText());
        assertThat(trinoTable.getViewExpandedText().get()).isEqualTo(glueTable.viewExpandedText());
    }

    @Test
    public void testConvertTableWithoutTableType()
    {
        Table table = getGlueTestTable(testDatabase.name())
                .toBuilder()
                .tableType(null)
                .build();
        io.trino.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertThat(trinoTable.getTableType()).isEqualTo(EXTERNAL_TABLE.name());
    }

    @Test
    public void testConvertTableNullPartitions()
    {
        Table table = getGlueTestTable(testDatabase.name())
                .toBuilder()
                .partitionKeys((Collection<software.amazon.awssdk.services.glue.model.Column>) null)
                .build();
        io.trino.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertThat(trinoTable.getPartitionColumns().isEmpty()).isTrue();
    }

    @Test
    public void testConvertTableUppercaseColumnType()
    {
        software.amazon.awssdk.services.glue.model.Column uppercaseColumn = getGlueTestColumn()
                .toBuilder()
                .type("String")
                .build();

        Table table = testTable.toBuilder()
                        .storageDescriptor(testTable.storageDescriptor().toBuilder()
                                .columns(ImmutableList.of(uppercaseColumn))
                                .build())
                                .build();
        GlueToTrinoConverter.convertTable(table, testDatabase.name());
    }

    @Test
    public void testConvertPartition()
    {
        GluePartitionConverter converter = createPartitionConverter(testTable);
        io.trino.metastore.Partition trinoPartition = converter.apply(testPartition);
        assertThat(trinoPartition.getDatabaseName()).isEqualTo(testPartition.databaseName());
        assertThat(trinoPartition.getTableName()).isEqualTo(testPartition.tableName());
        assertColumnList(trinoPartition.getColumns(), testPartition.storageDescriptor().columns());
        assertThat(trinoPartition.getValues()).isEqualTo(testPartition.values());
        assertStorage(trinoPartition.getStorage(), testPartition.storageDescriptor());
        assertThat(trinoPartition.getParameters()).isEqualTo(getPartitionParameters(testPartition));
    }

    @Test
    public void testPartitionConversionMemoization()
    {
        String fakeS3Location = "s3://some-fake-location";

        Partition partition = testPartition.toBuilder()
                .storageDescriptor(testPartition.storageDescriptor().toBuilder()
                        .location(fakeS3Location)
                        .build())
                .build();

        //  Second partition to convert with equal (but not aliased) values
        Partition partitionTwo = getGlueTestPartition("" + testDatabase.name(), "" + testTable.name(), new ArrayList<>(partition.values()))
                .toBuilder()
                .storageDescriptor(partition.storageDescriptor()
                        .toBuilder()
                        .columns(new ArrayList<>(testPartition.storageDescriptor().columns()))
                        .bucketColumns(new ArrayList<>(testPartition.storageDescriptor().bucketColumns()))
                        .location(fakeS3Location)
                        .inputFormat(testPartition.storageDescriptor().inputFormat())
                        .outputFormat(testPartition.storageDescriptor().outputFormat())
                        .parameters(new HashMap<>(testPartition.storageDescriptor().parameters()))
                        .build())
                .build();

        GluePartitionConverter converter = createPartitionConverter(testTable);
        io.trino.metastore.Partition trinoPartition = converter.apply(testPartition);
        io.trino.metastore.Partition trinoPartition2 = converter.apply(partitionTwo);

        assertThat(trinoPartition).isNotSameAs(trinoPartition2);
        assertThat(trinoPartition2.getDatabaseName()).isSameAs(trinoPartition.getDatabaseName());
        assertThat(trinoPartition2.getTableName()).isSameAs(trinoPartition.getTableName());
        assertThat(trinoPartition2.getColumns()).isSameAs(trinoPartition.getColumns());
        assertThat(trinoPartition2.getParameters()).isSameAs(trinoPartition.getParameters());
        assertThat(trinoPartition2.getValues()).isNotSameAs(trinoPartition.getValues());

        Storage storage = trinoPartition.getStorage();
        Storage storage2 = trinoPartition2.getStorage();

        assertThat(storage2.getStorageFormat()).isSameAs(storage.getStorageFormat());
        assertThat(storage2.getBucketProperty()).isSameAs(storage.getBucketProperty());
        assertThat(storage2.getSerdeParameters()).isSameAs(storage.getSerdeParameters());
        assertThat(storage2.getLocation()).isNotSameAs(storage.getLocation());
    }

    @Test
    public void testDatabaseNullParameters()
    {
        Database database = testDatabase.toBuilder().parameters(null).build();
        assertThat(GlueToTrinoConverter.convertDatabase(database).getParameters()).isNotNull();
    }

    @Test
    public void testTableNullParameters()
    {
        Table table = testTable.toBuilder()
                .parameters(null)
                .storageDescriptor(testTable.storageDescriptor()
                        .toBuilder()
                        .parameters(null)
                        .build())
                .build();
        io.trino.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertThat(trinoTable.getParameters()).isNotNull();
        assertThat(trinoTable.getStorage().getSerdeParameters()).isNotNull();
    }

    @Test
    public void testIcebergTableNullStorageDescriptor()
    {
        Table table = testTable.toBuilder()
                    .parameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE))
                    .storageDescriptor((StorageDescriptor) null)
                    .build();
        io.trino.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
    }

    @Test
    public void testIcebergTableNonNullStorageDescriptor()
    {
        Table table = testTable
                .toBuilder()
                .parameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE))
                .build();
        assertThat(table.storageDescriptor()).isNotNull();
        io.trino.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
    }

    @Test
    public void testDeltaTableNullStorageDescriptor()
    {
        Table table = testTable
                .toBuilder()
                .parameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER))
                .build();
        io.trino.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
    }

    @Test
    public void testDeltaTableNonNullStorageDescriptor()
    {
        Table table = testTable
                .toBuilder()
                .parameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER))
                .build();
        assertThat(table.storageDescriptor()).isNotNull();
        io.trino.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertThat(trinoTable.getDataColumns().stream()
                .map(Column::getName)
                .collect(toImmutableSet())).isEqualTo(table.storageDescriptor().columns().stream()
                .map(software.amazon.awssdk.services.glue.model.Column::name)
                .collect(toImmutableSet()));
    }

    @Test
    public void testIcebergMaterializedViewNullStorageDescriptor()
    {
        Table testMaterializedView = getGlueTestTrinoMaterializedView(testDatabase.name());
        assertThat(testMaterializedView.storageDescriptor()).isNull();
        io.trino.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testMaterializedView, testDatabase.name());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
    }

    @Test
    public void testPartitionNullParameters()
    {
        Partition partition = testPartition.toBuilder()
                .parameters(null)
                .build();
        assertThat(createPartitionConverter(testTable).apply(partition).getParameters()).isNotNull();
    }

    private static void assertColumnList(List<Column> actual, List<software.amazon.awssdk.services.glue.model.Column> expected)
    {
        if (expected == null) {
            assertThat(actual).isNull();
        }
        assertThat(actual.size()).isEqualTo(expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(Column actual, software.amazon.awssdk.services.glue.model.Column expected)
    {
        assertThat(actual.getName()).isEqualTo(expected.name());
        assertThat(actual.getType().getHiveTypeName().toString()).isEqualTo(expected.type());
        assertThat(actual.getComment().get()).isEqualTo(expected.comment());
    }

    private static void assertStorage(Storage actual, StorageDescriptor expected)
    {
        assertThat(actual.getLocation()).isEqualTo(expected.location());
        assertThat(actual.getStorageFormat().getSerde()).isEqualTo(expected.serdeInfo().serializationLibrary());
        assertThat(actual.getStorageFormat().getInputFormat()).isEqualTo(expected.inputFormat());
        assertThat(actual.getStorageFormat().getOutputFormat()).isEqualTo(expected.outputFormat());
        if (expected.bucketColumns().isEmpty()) {
            HiveBucketProperty bucketProperty = actual.getBucketProperty().get();
            assertThat(bucketProperty.bucketedBy()).isEqualTo(expected.bucketColumns());
            assertThat(bucketProperty.bucketCount()).isEqualTo(expected.numberOfBuckets().intValue());
        }
    }
}
