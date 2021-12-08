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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.DoubleType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.plugin.prometheus.PrometheusClient.TIMESTAMP_COLUMN_TYPE;
import static io.trino.plugin.prometheus.PrometheusQueryRunner.createPrometheusClient;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Integration tests against Prometheus container
 */
@Test(singleThreaded = true)
public class TestPrometheusIntegrationMetrics
{
    private static final PrometheusTableHandle RUNTIME_DETERMINED_TABLE_HANDLE = new PrometheusTableHandle("default", "up");

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
    public void testHandleErrorResponse()
    {
        assertThatThrownBy(() -> client.getTableNames("unknown"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Prometheus did no return metrics list (table names)");
        PrometheusTable table = client.getTable("unknown", "up");
        assertNull(table);
    }

    @Test
    public void testListSchemaNames()
    {
        PrometheusMetadata metadata = new PrometheusMetadata(client);
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableSet.of("default"));
    }

    @Test
    public void testGetColumnMetadata()
    {
        PrometheusMetadata metadata = new PrometheusMetadata(client);
        assertEquals(metadata.getColumnMetadata(SESSION, RUNTIME_DETERMINED_TABLE_HANDLE, new PrometheusColumnHandle("text", createUnboundedVarcharType(), 0)),
                new ColumnMetadata("text", createUnboundedVarcharType()));

        // prometheus connector assumes that the table handle and column handle are
        // properly formed, so it will return a metadata object for any
        // PrometheusTableHandle and PrometheusColumnHandle passed in.  This is on because
        // it is not possible for the Trino Metadata system to create the handles
        // directly.
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testCreateTable()
    {
        PrometheusMetadata metadata = new PrometheusMetadata(client);
        metadata.createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("default", "foo"),
                        ImmutableList.of(new ColumnMetadata("text", createUnboundedVarcharType()))),
                false);
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testDropTableTable()
    {
        PrometheusMetadata metadata = new PrometheusMetadata(client);
        metadata.dropTable(SESSION, RUNTIME_DETERMINED_TABLE_HANDLE);
    }

    @Test
    public void testGetColumnTypes()
    {
        String dataUri = server.getUri().toString();
        RecordSet recordSet = new PrometheusRecordSet(
                client,
                new PrometheusSplit(dataUri),
                ImmutableList.of(
                        new PrometheusColumnHandle("labels", createUnboundedVarcharType(), 0),
                        new PrometheusColumnHandle("value", DoubleType.DOUBLE, 1),
                        new PrometheusColumnHandle("timestamp", TIMESTAMP_COLUMN_TYPE, 2)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(createUnboundedVarcharType(), DoubleType.DOUBLE, TIMESTAMP_COLUMN_TYPE));

        recordSet = new PrometheusRecordSet(
                client,
                new PrometheusSplit(dataUri),
                ImmutableList.of(
                        new PrometheusColumnHandle("value", BIGINT, 1),
                        new PrometheusColumnHandle("text", createUnboundedVarcharType(), 0)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, createUnboundedVarcharType()));

        recordSet = new PrometheusRecordSet(
                client,
                new PrometheusSplit(dataUri),
                ImmutableList.of(
                        new PrometheusColumnHandle("value", BIGINT, 1),
                        new PrometheusColumnHandle("value", BIGINT, 1),
                        new PrometheusColumnHandle("text", createUnboundedVarcharType(), 0)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, BIGINT, createUnboundedVarcharType()));

        recordSet = new PrometheusRecordSet(client, new PrometheusSplit(dataUri), ImmutableList.of());
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of());
    }
}
