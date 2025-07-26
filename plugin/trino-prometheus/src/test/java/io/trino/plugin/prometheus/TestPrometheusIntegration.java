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

import io.airlift.units.Duration;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.concurrent.TimeUnit;

import static io.trino.plugin.prometheus.PrometheusQueryRunner.DEFAULT_NUMBER_OF_REQUESTS_WITHOUT_TIME_FILTER;
import static io.trino.plugin.prometheus.PrometheusQueryRunner.createPrometheusClient;
import static io.trino.plugin.prometheus.TestPrometheusTableHandle.newTableHandle;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// We use nginx proxy to record actual PromQL queries sent to Prometheus, so we can't run these tests in parallel.
@Execution(SAME_THREAD)
class TestPrometheusIntegration
        extends AbstractTestQueryFramework
{
    private static final int NUMBER_MORE_THAN_EXPECTED_NUMBER_SPLITS = 100;

    private PrometheusServer server;
    private PrometheusClient client;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.server = closeAfterClass(new PrometheusServer(PrometheusServer.DEFAULT_PROMETHEUS_VERSION, false, true));
        this.client = createPrometheusClient(server);
        return PrometheusQueryRunner.builder(server).build();
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
    public void testTimestampPushDown()
    {
        // default interval on the `up` metric that Prometheus records on itself is about 15 seconds, so this should only yield one or two row
        MaterializedResult results = computeActual("SELECT * FROM prometheus.default.up WHERE timestamp > (NOW() - INTERVAL '15' SECOND)");
        assertThat(results).hasSizeBetween(1, 2);
    }

    @Test
    public void testLabelEqPushDown()
    {
        RequestsRecorder recorder = new RequestsRecorder();
        MaterializedResult results = server.recordRequestsFor(recorder, () ->
                getQueryRunner().execute("SELECT * FROM prometheus.default.up WHERE labels['job'] = 'prometheus'"));

        assertThat(recorder.getRequestsWithoutDiscovery()).hasSize(DEFAULT_NUMBER_OF_REQUESTS_WITHOUT_TIME_FILTER);
        recorder.getRequestsWithoutDiscovery().forEach(request -> {
            assertThat(request.method).isEqualTo("GET");
            assertThat(request.extractPromQL()).isEqualTo("up{job=\"prometheus\"}[1d]");
        });

        // Prometheus reports the `up` metric every 15 seconds, so we expect at least one row, but since the time is unbounded, there could be even more.
        assertThat(results).hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    public void testLabelEqTimestampPushDown()
    {
        RequestsRecorder recorder = new RequestsRecorder();
        MaterializedResult results = server.recordRequestsFor(recorder, () ->
                getQueryRunner().execute("SELECT * FROM prometheus.default.up WHERE labels['job'] = 'prometheus' AND timestamp > (NOW() - INTERVAL '15' SECOND)"));

        assertThat(recorder.getRequestsWithoutDiscovery()).hasSize(1);
        LoggedRequest request = recorder.getRequestsWithoutDiscovery().getFirst();

        assertThat(request.method).isEqualTo("GET");
        assertThat(request.extractPromQL()).isEqualTo("up{job=\"prometheus\"}[1d]");

        // Prometheus reports the `up` metric every 15 seconds, so we expect at least one row, but at most two rows.
        assertThat(results).hasSizeBetween(1, 2);
    }

    @Test
    public void testLabelInPushDown()
    {
        RequestsRecorder recorder = new RequestsRecorder();
        MaterializedResult results = server.recordRequestsFor(recorder, () ->
                getQueryRunner().execute("SELECT * FROM prometheus.default.up WHERE labels['job'] IN ('prometheus','victoriametrics') AND timestamp > (NOW() - INTERVAL '15' SECOND)"));

        assertThat(recorder.getRequestsWithoutDiscovery()).hasSize(1);
        LoggedRequest request = recorder.getRequestsWithoutDiscovery().getFirst();

        assertThat(request.method).isEqualTo("GET");
        assertThat(request.extractPromQL()).isEqualTo("up{job=~\"(prometheus|victoriametrics)\"}[1d]");

        // Prometheus reports the `up` metric every 15 seconds, so we expect at least one row, but at most two rows.
        assertThat(results).hasSizeBetween(1, 2);
    }

    @Test
    public void testLabelLikePushDown()
    {
        RequestsRecorder recorder = new RequestsRecorder();
        MaterializedResult results = server.recordRequestsFor(recorder, () ->
                getQueryRunner().execute("SELECT * FROM prometheus.default.up WHERE labels['job'] LIKE 'pr_m%' AND timestamp > (NOW() - INTERVAL '15' SECOND)"));

        assertThat(recorder.getRequestsWithoutDiscovery()).hasSize(1);
        LoggedRequest request = recorder.getRequestsWithoutDiscovery().getFirst();

        assertThat(request.method).isEqualTo("GET");
        assertThat(request.extractPromQL()).isEqualTo("up{job=~\"pr.m.*\"}[1d]");

        // Prometheus reports the `up` metric every 15 seconds, so we expect at least one row, but at most two rows.
        assertThat(results).hasSizeBetween(1, 2);
    }

    @Test
    @Disabled // Not implemented yet
    public void testLabelNotLikePushDown()
    {
        RequestsRecorder recorder = new RequestsRecorder();
        MaterializedResult results = server.recordRequestsFor(recorder, () ->
                getQueryRunner().execute("SELECT * FROM prometheus.default.up WHERE labels['job'] NOT LIKE 'vict_ria%' AND timestamp > (NOW() - INTERVAL '15' SECOND)"));

        assertThat(recorder.getRequestsWithoutDiscovery()).hasSize(1);
        LoggedRequest request = recorder.getRequestsWithoutDiscovery().getFirst();

        assertThat(request.method).isEqualTo("GET");
        assertThat(request.extractPromQL()).isEqualTo("up{job!~\"vict.ria.*\"}[1d]");

        assertThat(results).hasSizeBetween(1, 2);
    }

    @Test
    public void testLabelLikeEscapePushDown()
    {
        RequestsRecorder recorder = new RequestsRecorder();
        MaterializedResult results = server.recordRequestsFor(recorder, () ->
                getQueryRunner().execute("SELECT * FROM prometheus.default.up WHERE labels['job'] LIKE '%pr_m\\%' ESCAPE '\\' AND timestamp > (NOW() - INTERVAL '15' SECOND)"));

        assertThat(recorder.getRequestsWithoutDiscovery()).hasSize(1);
        LoggedRequest request = recorder.getRequestsWithoutDiscovery().getFirst();

        assertThat(request.method).isEqualTo("GET");
        assertThat(request.extractPromQL()).isEqualTo("up{job=~\".*pr.m%\"}[1d]");

        assertThat(results).isEmpty();
    }

    @Test
    public void testLabelLikeAndEqAndNeqPushDown()
    {
        RequestsRecorder recorder = new RequestsRecorder();
        MaterializedResult results = server.recordRequestsFor(recorder, () ->
                getQueryRunner().execute("SELECT * FROM prometheus.default.up WHERE labels['job'] LIKE 'prom%' AND labels['instance'] = 'localhost:9090' AND labels['instance'] != 'localhost:8080' AND timestamp > (NOW() - INTERVAL '15' SECOND)"));

        assertThat(recorder.getRequestsWithoutDiscovery()).hasSize(1);
        LoggedRequest request = recorder.getRequestsWithoutDiscovery().getFirst();

        assertThat(request.method).isEqualTo("GET");
        assertThat(request.extractPromQL()).isEqualTo("up{job=~\"prom.*\",instance=\"localhost:9090\",instance!=\"localhost:8080\"}[1d]");

        // Prometheus reports the `up` metric every 15 seconds, so we expect at least one row, but at most two rows.
        assertThat(results).hasSizeBetween(1, 2);
    }

    @Test
    public void testLabelLikeAndEqAndNeqNegatedPushDown()
    {
        // This test deliberately uses a negated query to check that we don't push down the label filters in case there is a NOT modifier for AND-ed predicates.
        RequestsRecorder recorder = new RequestsRecorder();
        MaterializedResult results = server.recordRequestsFor(recorder, () ->
                getQueryRunner().execute("SELECT * FROM prometheus.default.up WHERE NOT (labels['job'] LIKE 'prom%' AND labels['instance'] = 'localhost:9090' AND labels['__name__'] != 'down') AND timestamp > (NOW() - INTERVAL '15' SECOND)"));

        assertThat(recorder.getRequestsWithoutDiscovery()).hasSize(1);
        LoggedRequest request = recorder.getRequestsWithoutDiscovery().getFirst();

        assertThat(request.method).isEqualTo("GET");
        assertThat(request.extractPromQL()).isEqualTo("up[1d]");

        assertThat(results).isEmpty();
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

    // TODO rewrite this test based on query.
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
        PrometheusSessionProperties sessionProperties = new PrometheusSessionProperties(config);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
        ConnectorSplitSource splits = splitManager.getSplits(
                null,
                session,
                newTableHandle("default", table.name()),
                (DynamicFilter) null,
                Constraint.alwaysTrue());
        int numSplits = splits.getNextBatch(NUMBER_MORE_THAN_EXPECTED_NUMBER_SPLITS).getNow(null).getSplits().size();
        assertThat((double) numSplits).isEqualTo(config.getMaxQueryRangeDuration().getValue(TimeUnit.SECONDS) / config.getQueryChunkSizeDuration().getValue(TimeUnit.SECONDS));
    }
}
