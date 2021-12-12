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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.prometheus.MetadataUtil.varcharMapType;
import static io.trino.plugin.prometheus.PrometheusClient.TIMESTAMP_COLUMN_TYPE;
import static io.trino.plugin.prometheus.PrometheusQueryRunner.createPrometheusClient;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Integration tests against Prometheus container
 */
@Test(singleThreaded = true)
public class TestPrometheusIntegrationSchema
{
    private static final PrometheusTableHandle RUNTIME_DETERMINED_TABLE_HANDLE = new PrometheusTableHandle("default", "up");

    private static final int NUMBER_MORE_THAN_EXPECTED_NUMBER_SPLITS = 100;

    private PrometheusServer server;
    private PrometheusClient client;

    @BeforeClass
    public void createQueryRunner()
            throws Exception
    {
        this.server = new PrometheusServer();
        this.client = createPrometheusClient(server);

        PrometheusServer.checkServerReady(this.client);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
    }

    @Test
    public void testRetrieveUpValue()
    {
        assertTrue(client.getTableNames("default").contains("up"), "Prometheus' own `up` metric should be available in default");
    }

    @Test
    public void testMetadata()
    {
        assertTrue(client.getTableNames("default").contains("up"));
        PrometheusTable table = client.getTable("default", "up");
        assertNotNull(table, "table is null");
        assertEquals(table.getName(), "up");
        assertEquals(table.getColumns(), ImmutableList.of(
                new PrometheusColumn("labels", varcharMapType),
                new PrometheusColumn("timestamp", TIMESTAMP_COLUMN_TYPE),
                new PrometheusColumn("value", DOUBLE)));
    }

    @Test
    public void testGetTableHandle()
    {
        PrometheusMetadata metadata = new PrometheusMetadata(client);
        assertEquals(metadata.getTableHandle(SESSION, new SchemaTableName("default", "up")), RUNTIME_DETERMINED_TABLE_HANDLE);
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("default", "unknown")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown")));
    }

    @Test
    public void testGetColumnHandles()
    {
        PrometheusMetadata metadata = new PrometheusMetadata(client);
        // known table
        assertEquals(metadata.getColumnHandles(SESSION, RUNTIME_DETERMINED_TABLE_HANDLE), ImmutableMap.of(
                "labels", new PrometheusColumnHandle("labels", createUnboundedVarcharType(), 0),
                "value", new PrometheusColumnHandle("value", DOUBLE, 1),
                "timestamp", new PrometheusColumnHandle("timestamp", TIMESTAMP_COLUMN_TYPE, 2)));

        // unknown table
        try {
            metadata.getColumnHandles(SESSION, new PrometheusTableHandle("unknown", "unknown"));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException expected) {
        }
        try {
            metadata.getColumnHandles(SESSION, new PrometheusTableHandle("default", "unknown"));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException expected) {
        }
    }

    @Test
    public void testGetTableMetadata()
    {
        PrometheusMetadata metadata = new PrometheusMetadata(client);
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, RUNTIME_DETERMINED_TABLE_HANDLE);
        assertEquals(tableMetadata.getTable(), new SchemaTableName("default", "up"));
        assertEquals(tableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("labels", varcharMapType),
                new ColumnMetadata("timestamp", TIMESTAMP_COLUMN_TYPE),
                new ColumnMetadata("value", DOUBLE)));

        // unknown tables should produce null
        assertNull(metadata.getTableMetadata(SESSION, new PrometheusTableHandle("unknown", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new PrometheusTableHandle("default", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new PrometheusTableHandle("unknown", "numbers")));
    }

    @Test
    public void testListTables()
    {
        PrometheusMetadata metadata = new PrometheusMetadata(client);
        assertTrue(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("default"))).contains(new SchemaTableName("default", "up")));

        // unknown schema
        assertThatThrownBy(() -> metadata.listTables(SESSION, Optional.of("unknown")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Prometheus did no return metrics list (table names): ");
    }

    @Test
    public void testCorrectNumberOfSplitsCreated()
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(server.getUri());
        config.setMaxQueryRangeDuration(new Duration(21, DAYS));
        config.setQueryChunkSizeDuration(new Duration(1, DAYS));
        config.setCacheDuration(new Duration(30, SECONDS));
        PrometheusTable table = client.getTable("default", "up");
        PrometheusSplitManager splitManager = new PrometheusSplitManager(client, new PrometheusClock(), config);
        ConnectorSplitSource splits = splitManager.getSplits(
                null,
                null,
                new PrometheusTableHandle("default", table.getName()),
                null,
                (DynamicFilter) null);
        int numSplits = splits.getNextBatch(NOT_PARTITIONED, NUMBER_MORE_THAN_EXPECTED_NUMBER_SPLITS).getNow(null).getSplits().size();
        assertEquals(numSplits, config.getMaxQueryRangeDuration().getValue(TimeUnit.SECONDS) / config.getQueryChunkSizeDuration().getValue(TimeUnit.SECONDS),
                0.001);
    }
}
