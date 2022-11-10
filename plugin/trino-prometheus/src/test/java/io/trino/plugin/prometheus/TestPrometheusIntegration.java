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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static io.trino.plugin.prometheus.PrometheusQueryRunner.createPrometheusClient;
import static io.trino.plugin.prometheus.PrometheusQueryRunner.createPrometheusQueryRunner;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestPrometheusIntegration
        extends AbstractTestQueryFramework
{
    private static final int NUMBER_MORE_THAN_EXPECTED_NUMBER_SPLITS = 100;

    private PrometheusServer server;
    private PrometheusClient client;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.server = closeAfterClass(new PrometheusServer());
        this.client = createPrometheusClient(server);
        return createPrometheusQueryRunner(server, ImmutableMap.of(), ImmutableMap.of());
    }

    @Test
    public void testSelectTable()
    {
        assertThat(query("SELECT labels FROM prometheus.default.up LIMIT 1"))
                .matches("SELECT MAP(ARRAY[VARCHAR 'instance', '__name__', 'job'], ARRAY[VARCHAR 'localhost:9090', 'up', 'prometheus'])");
    }

    @Test
    public void testAggregation()
    {
        assertQuerySucceeds("SELECT count(*) FROM default.up"); // Don't check value since the row number isn't deterministic
        assertQuery("SELECT avg(value) FROM default.up", "VALUES ('1.0')");
    }

    @Test
    public void testPushDown()
    {
        // default interval on the `up` metric that Prometheus records on itself is about 15 seconds, so this should only yield one or two row
        MaterializedResult results = computeActual("SELECT * FROM prometheus.default.up WHERE timestamp > (NOW() - INTERVAL '15' SECOND)");
        assertThat(results).hasSizeBetween(1, 2);
    }

    @Test
    public void testShowTables()
    {
        assertQuery("SHOW TABLES IN default LIKE 'up'", "VALUES 'up'");
    }

    @Test
    public void testShowCreateSchema()
    {
        assertQuery("SHOW CREATE SCHEMA default", "VALUES 'CREATE SCHEMA prometheus.default'");
        assertQueryFails("SHOW CREATE SCHEMA unknown", ".*Schema 'prometheus.unknown' does not exist");
    }

    @Test
    public void testListSchemaNames()
    {
        assertQuery("SHOW SCHEMAS LIKE 'default'", "VALUES 'default'");
    }

    @Test
    public void testCreateTable()
    {
        assertQueryFails("CREATE TABLE default.foo (text VARCHAR)", "This connector does not support creating tables");
    }

    @Test
    public void testDropTable()
    {
        assertQueryFails("DROP TABLE default.up", "This connector does not support dropping tables");
    }

    @Test
    public void testDescribeTable()
    {
        assertQuery("DESCRIBE default.up",
                "VALUES " +
                        "('labels', 'map(varchar, varchar)', '', '')," +
                        "('timestamp', 'timestamp(3) with time zone', '', '')," +
                        "('value', 'double', '', '')");
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
                (DynamicFilter) null,
                Constraint.alwaysTrue());
        int numSplits = splits.getNextBatch(NUMBER_MORE_THAN_EXPECTED_NUMBER_SPLITS).getNow(null).getSplits().size();
        assertEquals(numSplits, config.getMaxQueryRangeDuration().getValue(TimeUnit.SECONDS) / config.getQueryChunkSizeDuration().getValue(TimeUnit.SECONDS),
                0.001);
    }
}
