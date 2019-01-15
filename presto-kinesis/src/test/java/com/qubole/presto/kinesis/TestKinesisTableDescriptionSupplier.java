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
package com.qubole.presto.kinesis;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.qubole.presto.kinesis.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit test for the TableDescriptionSupplier and related classes
 */
public class TestKinesisTableDescriptionSupplier
{
    private Injector injector;

    @BeforeClass
    public void start()
    {
        // Create dependent objects, including the minimal config needed for this test
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kinesis.table-description-dir", "etc/kinesis")
                .put("kinesis.default-schema", "kinesis")
                .put("kinesis.hide-internal-columns", "false")
                .build();

        KinesisPlugin kinesisPlugin = TestUtils.createPluginInstance();
        TestUtils.createConnector(kinesisPlugin, properties, true);

        injector = kinesisPlugin.getInjector();
        assertNotNull(injector);
    }

    @Test
    public void testTableDefinition()
    {
        // Get the supplier from the injector
        KinesisTableDescriptionSupplier supplier = TestUtils.getTableDescSupplier(injector);
        assertNotNull(supplier);

        // Read table definition and verify
        Map<SchemaTableName, KinesisStreamDescription> readMap = supplier.get();
        assertTrue(!readMap.isEmpty());

        SchemaTableName tblName = new SchemaTableName("prod", "test_table");
        KinesisStreamDescription desc = readMap.get(tblName);

        assertNotNull(desc);
        assertEquals(desc.getSchemaName(), "prod");
        assertEquals(desc.getTableName(), "test_table");
        assertEquals(desc.getStreamName(), "test_kinesis_stream");
        assertNotNull(desc.getMessage());

        // Obtain the message part and verify we can read its fields
        KinesisStreamFieldGroup grp = desc.getMessage();
        assertEquals(grp.getDataFormat(), "json");
        List<KinesisStreamFieldDescription> fieldList = grp.getFields();
        assertEquals(fieldList.size(), 4); // (4 fields in test_table.json)
    }

    @Test
    public void testRelatedObjects()
    {
        // Metadata has a handle to the supplier, ensure it can use it correctly
        KinesisMetadata meta = injector.getInstance(KinesisMetadata.class);
        assertNotNull(meta);

        Map<SchemaTableName, KinesisStreamDescription> tblMap = meta.getDefinedTables();
        SchemaTableName tblName = new SchemaTableName("prod", "test_table");
        KinesisStreamDescription desc = tblMap.get(tblName);
        assertNotNull(desc);
        assertEquals(desc.getSchemaName(), "prod");
        assertEquals(desc.getTableName(), "test_table");
        assertEquals(desc.getStreamName(), "test_kinesis_stream");

        List<String> schemas = meta.listSchemaNames(null);
        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), "prod");

        KinesisTableHandle tblHandle = meta.getTableHandle(null, tblName);
        assertNotNull(tblHandle);
        assertEquals(tblHandle.getSchemaName(), "prod");
        assertEquals(tblHandle.getTableName(), "test_table");
        assertEquals(tblHandle.getStreamName(), "test_kinesis_stream");
        assertEquals(tblHandle.getMessageDataFormat(), "json");

        ConnectorTableMetadata tblMeta = meta.getTableMetadata(null, tblHandle);
        assertNotNull(tblMeta);
        assertEquals(tblMeta.getTable().getSchemaName(), "prod");
        assertEquals(tblMeta.getTable().getTableName(), "test_table");
        List<ColumnMetadata> columnList = tblMeta.getColumns();
        assertNotNull(columnList);

        boolean foundServiceType = false;
        boolean foundPartitionKey = false;
        for (ColumnMetadata column : columnList) {
            if (column.getName().equals("service_type")) {
                foundServiceType = true;
                assertEquals(column.getType().getDisplayName(), "varchar(20)");
            }
            if (column.getName().equals("_partition_key")) {
                foundPartitionKey = true;
                assertEquals(column.getType().getDisplayName(), "varchar");
            }
        }
        assertTrue(foundServiceType);
        assertTrue(foundPartitionKey);
    }
}
