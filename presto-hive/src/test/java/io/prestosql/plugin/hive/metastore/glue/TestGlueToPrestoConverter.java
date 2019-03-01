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
package io.prestosql.plugin.hive.metastore.glue;

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.glue.converter.GlueToPrestoConverter;
import io.prestosql.spi.security.PrincipalType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.amazonaws.util.CollectionUtils.isNullOrEmpty;
import static io.prestosql.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestColumn;
import static io.prestosql.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestDatabase;
import static io.prestosql.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestPartition;
import static io.prestosql.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestTable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestGlueToPrestoConverter
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

    @Test
    public void testConvertDatabase()
    {
        io.prestosql.plugin.hive.metastore.Database prestoDatabase = GlueToPrestoConverter.convertDatabase(testDatabase);
        assertEquals(prestoDatabase.getDatabaseName(), testDatabase.getName());
        assertEquals(prestoDatabase.getLocation().get(), testDatabase.getLocationUri());
        assertEquals(prestoDatabase.getComment().get(), testDatabase.getDescription());
        assertEquals(prestoDatabase.getParameters(), testDatabase.getParameters());
        assertEquals(prestoDatabase.getOwnerName(), PUBLIC_OWNER);
        assertEquals(prestoDatabase.getOwnerType(), PrincipalType.ROLE);
    }

    @Test
    public void testConvertTable()
    {
        io.prestosql.plugin.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(testTable, testDatabase.getName());
        assertEquals(prestoTable.getTableName(), testTable.getName());
        assertEquals(prestoTable.getDatabaseName(), testDatabase.getName());
        assertEquals(prestoTable.getTableType(), testTable.getTableType());
        assertEquals(prestoTable.getOwner(), testTable.getOwner());
        assertEquals(prestoTable.getParameters(), testTable.getParameters());
        assertColumnList(prestoTable.getDataColumns(), testTable.getStorageDescriptor().getColumns());
        assertColumnList(prestoTable.getPartitionColumns(), testTable.getPartitionKeys());
        assertStorage(prestoTable.getStorage(), testTable.getStorageDescriptor());
        assertEquals(prestoTable.getViewOriginalText().get(), testTable.getViewOriginalText());
        assertEquals(prestoTable.getViewExpandedText().get(), testTable.getViewExpandedText());
    }

    @Test
    public void testConvertTableNullPartitions()
    {
        testTable.setPartitionKeys(null);
        io.prestosql.plugin.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(testTable, testDatabase.getName());
        assertTrue(prestoTable.getPartitionColumns().isEmpty());
    }

    @Test
    public void testConvertTableUppercaseColumnType()
    {
        com.amazonaws.services.glue.model.Column uppercaseColumn = getGlueTestColumn().withType("String");
        testTable.getStorageDescriptor().setColumns(ImmutableList.of(uppercaseColumn));
        GlueToPrestoConverter.convertTable(testTable, testDatabase.getName());
    }

    @Test
    public void testConvertPartition()
    {
        io.prestosql.plugin.hive.metastore.Partition prestoPartition = GlueToPrestoConverter.convertPartition(testPartition);
        assertEquals(prestoPartition.getDatabaseName(), testPartition.getDatabaseName());
        assertEquals(prestoPartition.getTableName(), testPartition.getTableName());
        assertColumnList(prestoPartition.getColumns(), testPartition.getStorageDescriptor().getColumns());
        assertEquals(prestoPartition.getValues(), testPartition.getValues());
        assertStorage(prestoPartition.getStorage(), testPartition.getStorageDescriptor());
        assertEquals(prestoPartition.getParameters(), testPartition.getParameters());
    }

    @Test
    public void testDatabaseNullParameters()
    {
        testDatabase.setParameters(null);
        assertNotNull(GlueToPrestoConverter.convertDatabase(testDatabase).getParameters());
    }

    @Test
    public void testTableNullParameters()
    {
        testTable.setParameters(null);
        testTable.getStorageDescriptor().getSerdeInfo().setParameters(null);
        io.prestosql.plugin.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(testTable, testDatabase.getName());
        assertNotNull(prestoTable.getParameters());
        assertNotNull(prestoTable.getStorage().getSerdeParameters());
    }

    @Test
    public void testPartitionNullParameters()
    {
        testPartition.setParameters(null);
        assertNotNull(GlueToPrestoConverter.convertPartition(testPartition).getParameters());
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
        assertEquals(actual.getStorageFormat().getSerDe(), expected.getSerdeInfo().getSerializationLibrary());
        assertEquals(actual.getStorageFormat().getInputFormat(), expected.getInputFormat());
        assertEquals(actual.getStorageFormat().getOutputFormat(), expected.getOutputFormat());
        if (!isNullOrEmpty(expected.getBucketColumns())) {
            HiveBucketProperty bucketProperty = actual.getBucketProperty().get();
            assertEquals(bucketProperty.getBucketedBy(), expected.getBucketColumns());
            assertEquals(bucketProperty.getBucketCount(), expected.getNumberOfBuckets().intValue());
        }
    }
}
