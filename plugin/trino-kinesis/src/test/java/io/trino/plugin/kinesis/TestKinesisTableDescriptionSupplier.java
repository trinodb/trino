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
import com.google.common.io.Resources;
import io.trino.plugin.kinesis.util.KinesisTestClientManager;
import io.trino.plugin.kinesis.util.MockKinesisClient;
import io.trino.plugin.kinesis.util.TestUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestKinesisTableDescriptionSupplier
{
    private KinesisConnector connector;

    @BeforeAll
    public void start()
    {
        // Create dependent objects, including the minimal config needed for this test
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kinesis.table-description-location", Resources.getResource("etc/kinesis").getPath())
                .put("kinesis.default-schema", "kinesis")
                .put("kinesis.hide-internal-columns", "true")
                .put("bootstrap.quiet", "true")
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
        KinesisMetadata metadata = (KinesisMetadata) connector.getMetadata(SESSION, new ConnectorTransactionHandle() {});
        SchemaTableName tblName = new SchemaTableName("prod", "test_table");
        KinesisTableHandle tableHandle = metadata.getTableHandle(SESSION, tblName);
        assertThat(metadata).isNotNull();
        SchemaTableName tableSchemaName = tableHandle.schemaTableName();
        assertThat(tableSchemaName.getSchemaName()).isEqualTo("prod");
        assertThat(tableSchemaName.getTableName()).isEqualTo("test_table");
        assertThat(tableHandle.streamName()).isEqualTo("test_kinesis_stream");
        assertThat(tableHandle.messageDataFormat()).isEqualTo("json");
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle);
        assertThat(columnHandles.size()).isEqualTo(14);
        assertThat(columnHandles.values().stream().filter(x -> ((KinesisColumnHandle) x).isInternal()).count()).isEqualTo(10);
    }

    @Test
    public void testRelatedObjects()
    {
        KinesisMetadata metadata = (KinesisMetadata) connector.getMetadata(SESSION, new ConnectorTransactionHandle() {});
        assertThat(metadata).isNotNull();

        SchemaTableName tblName = new SchemaTableName("prod", "test_table");
        List<String> schemas = metadata.listSchemaNames(null);
        assertThat(schemas.size()).isEqualTo(1);
        assertThat(schemas.get(0)).isEqualTo("prod");

        KinesisTableHandle tblHandle = metadata.getTableHandle(null, tblName);
        assertThat(tblHandle).isNotNull();
        assertThat(tblHandle.schemaName()).isEqualTo("prod");
        assertThat(tblHandle.tableName()).isEqualTo("test_table");
        assertThat(tblHandle.streamName()).isEqualTo("test_kinesis_stream");
        assertThat(tblHandle.messageDataFormat()).isEqualTo("json");

        ConnectorTableMetadata tblMeta = metadata.getTableMetadata(null, tblHandle);
        assertThat(tblMeta).isNotNull();
        assertThat(tblMeta.getTable().getSchemaName()).isEqualTo("prod");
        assertThat(tblMeta.getTable().getTableName()).isEqualTo("test_table");
        List<ColumnMetadata> columnList = tblMeta.getColumns();
        assertThat(columnList).isNotNull();

        boolean foundServiceType = false;
        boolean foundPartitionKey = false;
        for (ColumnMetadata column : columnList) {
            if (column.getName().equals("service_type")) {
                foundServiceType = true;
                assertThat(column.getType().getDisplayName()).isEqualTo("varchar(20)");
            }
            if (column.getName().equals("_partition_key")) {
                foundPartitionKey = true;
                assertThat(column.getType().getDisplayName()).isEqualTo("varchar");
            }
        }
        assertThat(foundServiceType).isTrue();
        assertThat(foundPartitionKey).isTrue();
    }
}
