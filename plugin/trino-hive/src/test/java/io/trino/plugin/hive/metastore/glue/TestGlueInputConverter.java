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
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.glue.converter.GlueInputConverter;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.List;

import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getPrestoTestDatabase;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getPrestoTestPartition;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getPrestoTestTable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestGlueInputConverter
{
    private final Database testDb = getPrestoTestDatabase();
    private final Table testTbl = getPrestoTestTable(testDb.getDatabaseName());
    private final Partition testPartition = getPrestoTestPartition(testDb.getDatabaseName(), testTbl.getTableName(), ImmutableList.of("val1"));

    @Test
    public void testConvertDatabase()
    {
        DatabaseInput dbInput = GlueInputConverter.convertDatabase(testDb);

        assertEquals(dbInput.name(), testDb.getDatabaseName());
        assertEquals(dbInput.description(), testDb.getComment().get());
        assertEquals(dbInput.locationUri(), testDb.getLocation().get());
        assertEquals(dbInput.parameters(), testDb.getParameters());
    }

    @Test
    public void testConvertTable()
    {
        TableInput tblInput = GlueInputConverter.convertTable(testTbl);

        assertEquals(tblInput.name(), testTbl.getTableName());
        assertEquals(tblInput.owner(), testTbl.getOwner().orElse(null));
        assertEquals(tblInput.tableType(), testTbl.getTableType());
        assertEquals(tblInput.parameters(), testTbl.getParameters());
        assertColumnList(tblInput.storageDescriptor().columns(), testTbl.getDataColumns());
        assertColumnList(tblInput.partitionKeys(), testTbl.getPartitionColumns());
        assertStorage(tblInput.storageDescriptor(), testTbl.getStorage());
        assertEquals(tblInput.viewExpandedText(), testTbl.getViewExpandedText().get());
        assertEquals(tblInput.viewOriginalText(), testTbl.getViewOriginalText().get());
    }

    @Test
    public void testConvertPartition()
    {
        PartitionInput partitionInput = GlueInputConverter.convertPartition(testPartition);

        assertEquals(partitionInput.parameters(), testPartition.getParameters());
        assertStorage(partitionInput.storageDescriptor(), testPartition.getStorage());
        assertEquals(partitionInput.values(), testPartition.getValues());
    }

    private static void assertColumnList(List<software.amazon.awssdk.services.glue.model.Column> actual, List<Column> expected)
    {
        if (expected == null) {
            assertNull(actual);
        }
        assertEquals(actual.size(), expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(software.amazon.awssdk.services.glue.model.Column actual, Column expected)
    {
        assertEquals(actual.name(), expected.getName());
        assertEquals(actual.type(), expected.getType().getHiveTypeName().toString());
        assertEquals(actual.comment(), expected.getComment().get());
    }

    private static void assertStorage(StorageDescriptor actual, Storage expected)
    {
        assertEquals(actual.location(), expected.getLocation());
        assertEquals(actual.serdeInfo().serializationLibrary(), expected.getStorageFormat().getSerde());
        assertEquals(actual.inputFormat(), expected.getStorageFormat().getInputFormat());
        assertEquals(actual.outputFormat(), expected.getStorageFormat().getOutputFormat());

        if (expected.getBucketProperty().isPresent()) {
            HiveBucketProperty bucketProperty = expected.getBucketProperty().get();
            assertEquals(actual.bucketColumns(), bucketProperty.getBucketedBy());
            assertEquals(actual.numberOfBuckets().intValue(), bucketProperty.getBucketCount());
        }
    }
}
