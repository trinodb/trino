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
package io.trino.plugin.kinesis;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.kinesis.util.KinesisTestClientManager;
import io.trino.plugin.kinesis.util.MockKinesisClient;
import io.trino.plugin.kinesis.util.TestUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestKinesisTableDescriptionSupplier
{
    private KinesisConnector connector;

    @BeforeClass
    public void start()
    {
        // Create dependent objects, including the minimal config needed for this test
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kinesis.table-description-location", "etc/kinesis")
                .put("kinesis.default-schema", "kinesis")
                .put("kinesis.hide-internal-columns", "true")
                .buildOrThrow();

        KinesisTestClientManager kinesisTestClientManager = new KinesisTestClientManager();
        MockKinesisClient mockClient = (MockKinesisClient) kinesisTestClientManager.getClient();
        mockClient.createStream("test123", 2);
        mockClient.createStream("sampleTable", 2);
        KinesisConnectorFactory kinesisConnectorFactory = new TestingKinesisConnectorFactory(kinesisTestClientManager);

        KinesisPlugin kinesisPlugin = new KinesisPlugin(kinesisConnectorFactory);
        connector = TestUtils.createConnector(kinesisPlugin, properties, true);
    }

    @Test
    public void testTableDefinition()
    {
        KinesisMetadata metadata = (KinesisMetadata) connector.getMetadata(new ConnectorTransactionHandle() {});
        SchemaTableName tblName = new SchemaTableName("prod", "test_table");
        KinesisTableHandle tableHandle = metadata.getTableHandle(SESSION, tblName);
        assertNotNull(metadata);
        SchemaTableName tableSchemaName = tableHandle.toSchemaTableName();
        assertEquals(tableSchemaName.getSchemaName(), "prod");
        assertEquals(tableSchemaName.getTableName(), "test_table");
        assertEquals(tableHandle.getStreamName(), "test_kinesis_stream");
        assertEquals(tableHandle.getMessageDataFormat(), "json");
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle);
        assertEquals(columnHandles.size(), 14);
        assertEquals(columnHandles.values().stream().filter(x -> ((KinesisColumnHandle) x).isInternal()).count(), 10);
    }

    @Test
    public void testRelatedObjects()
    {
        KinesisMetadata metadata = (KinesisMetadata) connector.getMetadata(new ConnectorTransactionHandle() {});
        assertNotNull(metadata);

        SchemaTableName tblName = new SchemaTableName("prod", "test_table");
        List<String> schemas = metadata.listSchemaNames(null);
        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), "prod");

        KinesisTableHandle tblHandle = metadata.getTableHandle(null, tblName);
        assertNotNull(tblHandle);
        assertEquals(tblHandle.getSchemaName(), "prod");
        assertEquals(tblHandle.getTableName(), "test_table");
        assertEquals(tblHandle.getStreamName(), "test_kinesis_stream");
        assertEquals(tblHandle.getMessageDataFormat(), "json");

        ConnectorTableMetadata tblMeta = metadata.getTableMetadata(null, tblHandle);
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
