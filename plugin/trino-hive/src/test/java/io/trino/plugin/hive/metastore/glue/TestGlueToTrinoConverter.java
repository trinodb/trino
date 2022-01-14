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

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter;
import io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.GluePartitionConverter;
import io.trino.spi.security.PrincipalType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static com.amazonaws.util.CollectionUtils.isNullOrEmpty;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestColumn;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestDatabase;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestPartition;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestStorageDescriptor;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestTable;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

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
        testTable = getGlueTestTable(testDatabase.getName());
        testPartition = getGlueTestPartition(testDatabase.getName(), testTable.getName(), ImmutableList.of("val1"));
    }

    private static GluePartitionConverter createPartitionConverter(Table table)
    {
        return new GluePartitionConverter(GlueToTrinoConverter.convertTable(table, table.getDatabaseName()));
    }

    @Test
    public void testConvertDatabase()
    {
        io.trino.plugin.hive.metastore.Database trinoDatabase = GlueToTrinoConverter.convertDatabase(testDatabase);
        assertEquals(trinoDatabase.getDatabaseName(), testDatabase.getName());
        assertEquals(trinoDatabase.getLocation().get(), testDatabase.getLocationUri());
        assertEquals(trinoDatabase.getComment().get(), testDatabase.getDescription());
        assertEquals(trinoDatabase.getParameters(), testDatabase.getParameters());
        assertEquals(trinoDatabase.getOwnerName(), Optional.of(PUBLIC_OWNER));
        assertEquals(trinoDatabase.getOwnerType(), Optional.of(PrincipalType.ROLE));
    }

    @Test
    public void testConvertTable()
    {
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testTable, testDatabase.getName());
        assertEquals(trinoTable.getTableName(), testTable.getName());
        assertEquals(trinoTable.getDatabaseName(), testDatabase.getName());
        assertEquals(trinoTable.getTableType(), testTable.getTableType());
        assertEquals(trinoTable.getOwner().orElse(null), testTable.getOwner());
        assertEquals(trinoTable.getParameters(), testTable.getParameters());
        assertColumnList(trinoTable.getDataColumns(), testTable.getStorageDescriptor().getColumns());
        assertColumnList(trinoTable.getPartitionColumns(), testTable.getPartitionKeys());
        assertStorage(trinoTable.getStorage(), testTable.getStorageDescriptor());
        assertEquals(trinoTable.getViewOriginalText().get(), testTable.getViewOriginalText());
        assertEquals(trinoTable.getViewExpandedText().get(), testTable.getViewExpandedText());
    }

    @Test
    public void testConvertTableWithOpenCSVSerDe()
    {
        Table glueTable = getGlueTestTable(testDatabase.getName());
        glueTable.setStorageDescriptor(getGlueTestStorageDescriptor(
                ImmutableList.of(getGlueTestColumn("int")),
                "org.apache.hadoop.hive.serde2.OpenCSVSerde"));
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(glueTable, testDatabase.getName());

        assertEquals(trinoTable.getTableName(), glueTable.getName());
        assertEquals(trinoTable.getDatabaseName(), testDatabase.getName());
        assertEquals(trinoTable.getTableType(), glueTable.getTableType());
        assertEquals(trinoTable.getOwner().orElse(null), glueTable.getOwner());
        assertEquals(trinoTable.getParameters(), glueTable.getParameters());
        assertEquals(trinoTable.getDataColumns().size(), 1);
        assertEquals(trinoTable.getDataColumns().get(0).getType(), HIVE_STRING);

        assertColumnList(trinoTable.getPartitionColumns(), glueTable.getPartitionKeys());
        assertStorage(trinoTable.getStorage(), glueTable.getStorageDescriptor());
        assertEquals(trinoTable.getViewOriginalText().get(), glueTable.getViewOriginalText());
        assertEquals(trinoTable.getViewExpandedText().get(), glueTable.getViewExpandedText());
    }

    @Test
    public void testConvertTableWithoutTableType()
    {
        Table table = getGlueTestTable(testDatabase.getName());
        table.setTableType(null);
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.getName());
        assertEquals(trinoTable.getTableType(), EXTERNAL_TABLE.name());
    }

    @Test
    public void testConvertTableNullPartitions()
    {
        testTable.setPartitionKeys(null);
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testTable, testDatabase.getName());
        assertTrue(trinoTable.getPartitionColumns().isEmpty());
    }

    @Test
    public void testConvertTableUppercaseColumnType()
    {
        com.amazonaws.services.glue.model.Column uppercaseColumn = getGlueTestColumn().withType("String");
        testTable.getStorageDescriptor().setColumns(ImmutableList.of(uppercaseColumn));
        GlueToTrinoConverter.convertTable(testTable, testDatabase.getName());
    }

    @Test
    public void testConvertPartition()
    {
        GluePartitionConverter converter = createPartitionConverter(testTable);
        io.trino.plugin.hive.metastore.Partition trinoPartition = converter.apply(testPartition);
        assertEquals(trinoPartition.getDatabaseName(), testPartition.getDatabaseName());
        assertEquals(trinoPartition.getTableName(), testPartition.getTableName());
        assertColumnList(trinoPartition.getColumns(), testPartition.getStorageDescriptor().getColumns());
        assertEquals(trinoPartition.getValues(), testPartition.getValues());
        assertStorage(trinoPartition.getStorage(), testPartition.getStorageDescriptor());
        assertEquals(trinoPartition.getParameters(), testPartition.getParameters());
    }

    @Test
    public void testPartitionConversionMemoization()
    {
        String fakeS3Location = "s3://some-fake-location";
        testPartition.getStorageDescriptor().setLocation(fakeS3Location);
        //  Second partition to convert with equal (but not aliased) values
        Partition partitionTwo = getGlueTestPartition("" + testDatabase.getName(), "" + testTable.getName(), new ArrayList<>(testPartition.getValues()));
        //  Ensure storage fields match as well
        partitionTwo.getStorageDescriptor().setColumns(new ArrayList<>(testPartition.getStorageDescriptor().getColumns()));
        partitionTwo.getStorageDescriptor().setBucketColumns(new ArrayList<>(testPartition.getStorageDescriptor().getBucketColumns()));
        partitionTwo.getStorageDescriptor().setLocation("" + fakeS3Location);
        partitionTwo.getStorageDescriptor().setInputFormat("" + testPartition.getStorageDescriptor().getInputFormat());
        partitionTwo.getStorageDescriptor().setOutputFormat("" + testPartition.getStorageDescriptor().getOutputFormat());
        partitionTwo.getStorageDescriptor().setParameters(new HashMap<>(testPartition.getStorageDescriptor().getParameters()));

        GluePartitionConverter converter = createPartitionConverter(testTable);
        io.trino.plugin.hive.metastore.Partition trinoPartition = converter.apply(testPartition);
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
        testDatabase.setParameters(null);
        assertNotNull(GlueToTrinoConverter.convertDatabase(testDatabase).getParameters());
    }

    @Test
    public void testTableNullParameters()
    {
        testTable.setParameters(null);
        testTable.getStorageDescriptor().getSerdeInfo().setParameters(null);
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testTable, testDatabase.getName());
        assertNotNull(trinoTable.getParameters());
        assertNotNull(trinoTable.getStorage().getSerdeParameters());
    }

    @Test
    public void testPartitionNullParameters()
    {
        testPartition.setParameters(null);
        assertNotNull(createPartitionConverter(testTable).apply(testPartition).getParameters());
    }

    private static void assertColumnList(List<Column> actual, List<com.amazonaws.services.glue.model.Column> expected)
    {
        if (expected == null) {
            assertNull(actual);
        }
        assertEquals(actual.size(), expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(Column actual, com.amazonaws.services.glue.model.Column expected)
    {
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getType().getHiveTypeName().toString(), expected.getType());
        assertEquals(actual.getComment().get(), expected.getComment());
    }

    private static void assertStorage(Storage actual, StorageDescriptor expected)
    {
        assertEquals(actual.getLocation(), expected.getLocation());
        assertEquals(actual.getStorageFormat().getSerde(), expected.getSerdeInfo().getSerializationLibrary());
        assertEquals(actual.getStorageFormat().getInputFormat(), expected.getInputFormat());
        assertEquals(actual.getStorageFormat().getOutputFormat(), expected.getOutputFormat());
        if (!isNullOrEmpty(expected.getBucketColumns())) {
            HiveBucketProperty bucketProperty = actual.getBucketProperty().get();
            assertEquals(bucketProperty.getBucketedBy(), expected.getBucketColumns());
            assertEquals(bucketProperty.getBucketCount(), expected.getNumberOfBuckets().intValue());
        }
    }
}
