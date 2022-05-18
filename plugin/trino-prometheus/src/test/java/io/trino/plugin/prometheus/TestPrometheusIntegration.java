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
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.type.DoubleType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.prometheus.MetadataUtil.varcharMapType;
import static io.trino.plugin.prometheus.PrometheusClient.TIMESTAMP_COLUMN_TYPE;
import static io.trino.plugin.prometheus.PrometheusQueryRunner.createPrometheusClient;
import static io.trino.plugin.prometheus.PrometheusQueryRunner.createPrometheusQueryRunner;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.*;
import static org.testng.Assert.fail;

/**
 * Integration tests against Prometheus container
 */
@Test(singleThreaded = true)
public class TestPrometheusIntegration extends AbstractTestQueryFramework
{
    private static final PrometheusTableHandle RUNTIME_DETERMINED_TABLE_HANDLE = new PrometheusTableHandle("default", "up");
    private static final int NUMBER_MORE_THAN_EXPECTED_NUMBER_SPLITS = 100;

    private PrometheusServer server;
    private PrometheusClient client;

    @BeforeClass
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.server = new PrometheusServer();
        this.client = createPrometheusClient(server);
        PrometheusServer.checkServerReady(this.client);
        return createPrometheusQueryRunner(server, ImmutableMap.of());
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

    @Test
    public void testConfirmMetricAvailableAndCheckUp()
            throws Exception
    {
        int maxTries = 60;
        int timeBetweenTriesMillis = 1000;
        int tries = 0;
        final OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(120, TimeUnit.SECONDS)
                .readTimeout(120, TimeUnit.SECONDS)
                .build();
        HttpUrl.Builder urlBuilder = HttpUrl.parse(server.getUri().toString()).newBuilder().encodedPath("/api/v1/query");
        urlBuilder.addQueryParameter("query", "up[1d]");
        String url = urlBuilder.build().toString();
        Request request = new Request.Builder()
                .url(url)
                .build();
        String responseBody;
        // this seems to be a reliable way to ensure Prometheus has `up` metric data
        while (tries < maxTries) {
            responseBody = httpClient.newCall(request).execute().body().string();
            if (responseBody.contains("values")) {
                Logger log = Logger.get(TestPrometheusIntegration.class);
                log.info("prometheus response: %s", responseBody);
                break;
            }
            Thread.sleep(timeBetweenTriesMillis);
            tries++;
        }
        if (tries == maxTries) {
            fail("Prometheus container not available for metrics query in " + maxTries * timeBetweenTriesMillis + " milliseconds.");
        }
        // now we're making sure the client is ready
        tries = 0;
        while (tries < maxTries) {
            if (getQueryRunner().tableExists(getSession(), "up")) {
                break;
            }
            Thread.sleep(timeBetweenTriesMillis);
            tries++;
        }
        if (tries == maxTries) {
            fail("Prometheus container, or client, not available for metrics query in " + maxTries * timeBetweenTriesMillis + " milliseconds.");
        }

        MaterializedResult results = computeActual("SELECT * FROM prometheus.default.up LIMIT 1");
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0).toString(), "{instance=localhost:9090, __name__=up, job=prometheus}");
    }

    @Test
    public void testPushDown()
    {
        // default interval on the `up` metric that Prometheus records on itself is about 15 seconds, so this should only yield one or two row
        MaterializedResult results = computeActual("SELECT * FROM prometheus.default.up WHERE timestamp > (NOW() - INTERVAL '15' SECOND)");
        assertThat(results).hasSizeBetween(1, 2);
    }
}
