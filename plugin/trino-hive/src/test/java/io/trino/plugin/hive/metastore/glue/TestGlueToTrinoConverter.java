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
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter;
import io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.GluePartitionConverter;
import io.trino.spi.security.PrincipalType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestColumn;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestDatabase;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestPartition;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestStorageDescriptor;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestTable;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestTrinoMaterializedView;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getPartitionParameters;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableParameters;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableTypeNullable;
import static io.trino.plugin.hive.util.HiveUtil.DELTA_LAKE_PROVIDER;
import static io.trino.plugin.hive.util.HiveUtil.ICEBERG_TABLE_TYPE_NAME;
import static io.trino.plugin.hive.util.HiveUtil.ICEBERG_TABLE_TYPE_VALUE;
import static io.trino.plugin.hive.util.HiveUtil.SPARK_TABLE_PROVIDER_KEY;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static software.amazon.awssdk.utils.CollectionUtils.isNullOrEmpty;

@Test(singleThreaded = true)
public class TestGlueToTrinoConverter
{
    private static final String PUBLIC_OWNER = "PUBLIC";

    private Database testDatabase;
    private Table testTable;
    private Partition testPartition;

    @BeforeMethod
    public void setup()
    {
        testDatabase = getGlueTestDatabase();
        testTable = getGlueTestTable(testDatabase.name());
        testPartition = getGlueTestPartition(testDatabase.name(), testTable.name(), ImmutableList.of("val1"));
    }

    private static GluePartitionConverter createPartitionConverter(Table table)
    {
        return new GluePartitionConverter(GlueToTrinoConverter.convertTable(table, table.databaseName()));
    }

    @Test
    public void testConvertDatabase()
    {
        io.trino.plugin.hive.metastore.Database trinoDatabase = GlueToTrinoConverter.convertDatabase(testDatabase);
        assertEquals(trinoDatabase.getDatabaseName(), testDatabase.name());
        assertEquals(trinoDatabase.getLocation().get(), testDatabase.locationUri());
        assertEquals(trinoDatabase.getComment().get(), testDatabase.description());
        assertEquals(trinoDatabase.getParameters(), testDatabase.parameters());
        assertEquals(trinoDatabase.getOwnerName(), Optional.of(PUBLIC_OWNER));
        assertEquals(trinoDatabase.getOwnerType(), Optional.of(PrincipalType.ROLE));
    }

    @Test
    public void testConvertTable()
    {
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testTable, testDatabase.name());
        assertEquals(trinoTable.getTableName(), testTable.name());
        assertEquals(trinoTable.getDatabaseName(), testDatabase.name());
        assertEquals(trinoTable.getTableType(), testTable.tableType());
        assertEquals(trinoTable.getOwner().orElse(null), testTable.owner());
        assertEquals(trinoTable.getParameters(), getTableParameters(testTable));
        assertColumnList(trinoTable.getDataColumns(), testTable.storageDescriptor().columns());
        assertColumnList(trinoTable.getPartitionColumns(), testTable.partitionKeys());
        assertStorage(trinoTable.getStorage(), testTable.storageDescriptor());
        assertEquals(trinoTable.getViewOriginalText().get(), testTable.viewOriginalText());
        assertEquals(trinoTable.getViewExpandedText().get(), testTable.viewExpandedText());
    }

    @Test
    public void testConvertTableWithOpenCSVSerDe()
    {
        Table glueTable = getGlueTestTable(testDatabase.name());
        glueTable = glueTable.toBuilder().storageDescriptor(
                getGlueTestStorageDescriptor(
                ImmutableList.of(getGlueTestColumn("int")),
                "org.apache.hadoop.hive.serde2.OpenCSVSerde"))
                .build();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(glueTable, testDatabase.name());

        assertEquals(trinoTable.getTableName(), glueTable.name());
        assertEquals(trinoTable.getDatabaseName(), testDatabase.name());
        assertEquals(trinoTable.getTableType(), getTableTypeNullable(glueTable));
        assertEquals(trinoTable.getOwner().orElse(null), glueTable.owner());
        assertEquals(trinoTable.getParameters(), getTableParameters(glueTable));
        assertEquals(trinoTable.getDataColumns().size(), 1);
        assertEquals(trinoTable.getDataColumns().get(0).getType(), HIVE_STRING);

        assertColumnList(trinoTable.getPartitionColumns(), glueTable.partitionKeys());
        assertStorage(trinoTable.getStorage(), glueTable.storageDescriptor());
        assertEquals(trinoTable.getViewOriginalText().get(), glueTable.viewOriginalText());
        assertEquals(trinoTable.getViewExpandedText().get(), glueTable.viewExpandedText());
    }

    @Test
    public void testConvertTableWithoutTableType()
    {
        Table table = getGlueTestTable(testDatabase.name());
        table = table.toBuilder().tableType(null).build();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertEquals(trinoTable.getTableType(), EXTERNAL_TABLE.name());
    }

    @Test
    public void testConvertTableNullPartitions()
    {
        Table table = testTable.toBuilder().partitionKeys((Collection<software.amazon.awssdk.services.glue.model.Column>) null).build();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertTrue(trinoTable.getPartitionColumns().isEmpty());
    }

    @Test
    public void testConvertTableUppercaseColumnType()
    {
        software.amazon.awssdk.services.glue.model.Column uppercaseColumn = getGlueTestColumn().toBuilder().type("String").build();
        StorageDescriptor sd = testTable.storageDescriptor();
        Table table = testTable.toBuilder()
                .storageDescriptor(sd.toBuilder()
                        .columns(ImmutableList.of(uppercaseColumn))
                        .build())
                .build();
        GlueToTrinoConverter.convertTable(table, testDatabase.name());
    }

    @Test
    public void testConvertPartition()
    {
        GluePartitionConverter converter = createPartitionConverter(testTable);
        io.trino.plugin.hive.metastore.Partition trinoPartition = converter.apply(testPartition);
        assertEquals(trinoPartition.getDatabaseName(), testPartition.databaseName());
        assertEquals(trinoPartition.getTableName(), testPartition.tableName());
        assertColumnList(trinoPartition.getColumns(), testPartition.storageDescriptor().columns());
        assertEquals(trinoPartition.getValues(), testPartition.values());
        assertStorage(trinoPartition.getStorage(), testPartition.storageDescriptor());
        assertEquals(trinoPartition.getParameters(), getPartitionParameters(testPartition));
    }

    @Test
    public void testPartitionConversionMemoization()
    {
        String fakeS3Location = "s3://some-fake-location";
        StorageDescriptor sd = testPartition.storageDescriptor();
        Partition partition = testPartition.toBuilder()
                .storageDescriptor(sd.toBuilder()
                        .location(fakeS3Location)
                        .build())
                .build();
        //  Second partition to convert with equal (but not aliased) values
        Partition partitionTwo = getGlueTestPartition(
                "" + testDatabase.name(),
                "" + testTable.name(),
                new ArrayList<>(testPartition.values()));
        //  Ensure storage fields match as well
        partitionTwo = partitionTwo.toBuilder().storageDescriptor(
                StorageDescriptor.builder()
                        .columns(new ArrayList<>(sd.columns()))
                        .bucketColumns(new ArrayList<>(sd.bucketColumns()))
                        .serdeInfo(partitionTwo.storageDescriptor().serdeInfo())
                        .location("" + fakeS3Location)
                        .inputFormat("" + sd.inputFormat())
                        .outputFormat("" + sd.outputFormat())
                        .parameters(new HashMap<>(sd.parameters()))
                        .numberOfBuckets(partitionTwo.storageDescriptor().numberOfBuckets())
                        .build())
                .build();

        GluePartitionConverter converter = createPartitionConverter(testTable);
        io.trino.plugin.hive.metastore.Partition trinoPartition = converter.apply(partition);
        io.trino.plugin.hive.metastore.Partition trinoPartition2 = converter.apply(partitionTwo);

        assertNotSame(trinoPartition, trinoPartition2);
        assertSame(trinoPartition2.getDatabaseName(), trinoPartition.getDatabaseName());
        assertSame(trinoPartition2.getTableName(), trinoPartition.getTableName());
        assertSame(trinoPartition2.getColumns(), trinoPartition.getColumns());
        assertSame(trinoPartition2.getParameters(), trinoPartition.getParameters());
        assertNotSame(trinoPartition2.getValues(), trinoPartition.getValues());

        Storage storage = trinoPartition.getStorage();
        Storage storage2 = trinoPartition2.getStorage();

        assertSame(storage2.getStorageFormat(), storage.getStorageFormat());
        assertSame(storage2.getBucketProperty(), storage.getBucketProperty());
        assertSame(storage2.getSerdeParameters(), storage.getSerdeParameters());
        assertNotSame(storage2.getLocation(), storage.getLocation());
    }

    @Test
    public void testDatabaseNullParameters()
    {
        Database database = testDatabase.toBuilder().parameters(null).build();
        assertNotNull(GlueToTrinoConverter.convertDatabase(database).getParameters());
    }

    @Test
    public void testTableNullParameters()
    {
        StorageDescriptor sd = testTable.storageDescriptor();
        SerDeInfo serDeInfo = sd.serdeInfo();
        Table table = testTable.toBuilder().parameters(null).storageDescriptor(
                sd.toBuilder().serdeInfo(
                        serDeInfo.toBuilder()
                                .parameters(null)
                                .build())
                        .build())
                .build();

        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertNotNull(trinoTable.getParameters());
        assertNotNull(trinoTable.getStorage().getSerdeParameters());
    }

    @Test
    public void testIcebergTableNullStorageDescriptor()
    {
        Table table = testTable.toBuilder()
                .parameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE))
                .storageDescriptor((StorageDescriptor) null)
                .build();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertEquals(trinoTable.getDataColumns().size(), 1);
    }

    @Test
    public void testIcebergTableNonNullStorageDescriptor()
    {
        Table table = testTable.toBuilder().parameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE)).build();
        assertNotNull(testTable.storageDescriptor());
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertEquals(trinoTable.getDataColumns().size(), 1);
    }

    @Test
    public void testDeltaTableNullStorageDescriptor()
    {
        Table table = testTable.toBuilder()
                .parameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER))
                .storageDescriptor((StorageDescriptor) null)
                .build();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertEquals(trinoTable.getDataColumns().size(), 1);
    }

    @Test
    public void testDeltaTableNonNullStorageDescriptor()
    {
        Table table = testTable.toBuilder().parameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER)).build();
        assertNotNull(testTable.storageDescriptor());
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.name());
        assertEquals(
                trinoTable.getDataColumns().stream()
                        .map(Column::getName)
                        .collect(toImmutableSet()),
                table.storageDescriptor().columns().stream()
                        .map(software.amazon.awssdk.services.glue.model.Column::name)
                        .collect(toImmutableSet()));
    }

    @Test
    public void testIcebergMaterializedViewNullStorageDescriptor()
    {
        Table testMaterializedView = getGlueTestTrinoMaterializedView(testDatabase.name());
        assertNull(testMaterializedView.storageDescriptor());
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testMaterializedView, testDatabase.name());
        assertEquals(trinoTable.getDataColumns().size(), 1);
    }

    @Test
    public void testPartitionNullParameters()
    {
        Partition partition = testPartition.toBuilder().parameters(null).build();
        assertNotNull(createPartitionConverter(testTable).apply(partition).getParameters());
    }

    private static void assertColumnList(List<Column> actual, List<software.amazon.awssdk.services.glue.model.Column> expected)
    {
        if (expected == null) {
            assertNull(actual);
        }
        assertEquals(actual.size(), expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(Column actual, software.amazon.awssdk.services.glue.model.Column expected)
    {
        assertEquals(actual.getName(), expected.name());
        assertEquals(actual.getType().getHiveTypeName().toString(), expected.type());
        assertEquals(actual.getComment().get(), expected.comment());
    }

    private static void assertStorage(Storage actual, StorageDescriptor expected)
    {
        assertEquals(actual.getLocation(), expected.location());
        assertEquals(actual.getStorageFormat().getSerde(), expected.serdeInfo().serializationLibrary());
        assertEquals(actual.getStorageFormat().getInputFormat(), expected.inputFormat());
        assertEquals(actual.getStorageFormat().getOutputFormat(), expected.outputFormat());
        if (!isNullOrEmpty(expected.bucketColumns())) {
            HiveBucketProperty bucketProperty = actual.getBucketProperty().get();
            assertEquals(bucketProperty.getBucketedBy(), expected.bucketColumns());
            assertEquals(bucketProperty.getBucketCount(), expected.numberOfBuckets().intValue());
        }
    }
}
