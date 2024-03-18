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

package io.trino.plugin.warp.it.proxiedconnector.passthrough;

import io.trino.plugin.varada.api.warmup.WarmUpType;
import io.trino.plugin.varada.api.warmup.WarmupPropertiesData;
import io.trino.plugin.varada.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.PASS_THROUGH_DISPATCHER;
import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.configuration.WarpExtensionConfiguration.USE_HTTP_SERVER_PORT;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHivePassThroughProxiedConnectorIntegrationSmokeIT
        extends DispatcherStubsIntegrationSmokeIT
{
    public TestHivePassThroughProxiedConnectorIntegrationSmokeIT()
    {
        super(4, "hive_passthrough");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DispatcherQueryRunner.createQueryRunner(storageEngineModule,
                Optional.empty(),
                numNodes,
                Collections.emptyMap(),
                Map.of("http-server.log.enabled", "false",
                        WARP_SPEED_PREFIX + USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "varada",
                        WARP_SPEED_PREFIX + PROXIED_CONNECTOR, HIVE_CONNECTOR_NAME,
                        WARP_SPEED_PREFIX + PASS_THROUGH_DISPATCHER, "hive,hudi"),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                catalog,
                new WarpPlugin(),
                Map.of());
    }

    @Test
    public void testSinglePartitionMultipleSplits()
    {
        try {
            computeActual("CREATE TABLE pt(id integer, a varchar, date_date date) " +
                    "WITH (format='PARQUET', partitioned_by = ARRAY['date_date'])");
            IntStream.range(1, 9).forEach(value -> assertUpdate(format("INSERT INTO pt(id, a, date_date) VALUES(%d, 'a-%d',CAST('2020-04-0%d' AS date))", value, value, value), 1));

            MaterializedResult materializedRows = computeActual("SELECT count(a) FROM pt WHERE date_date=CAST('2020-04-01' AS date)");
            assertThat(materializedRows.getRowCount()).isEqualTo(1);
            assertThat(materializedRows.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS pt");
        }
    }

    @Test
    public void testMultiplePartitionsMultipleSplits()
    {
        String table = "pt";
        String aCol = "a";
        String dateIntCol = "date_int";
        String dateDateCol = "date_date";
        try {
            computeActual(format("CREATE TABLE %s(%s varchar, %s integer, %s date) " +
                            "WITH (format='PARQUET', partitioned_by = ARRAY['%s', '%s'])",
                    table, aCol, dateIntCol, dateDateCol, dateIntCol, dateDateCol));

            IntStream.range(0, 2).forEach(indexDateInt -> IntStream.range(1, 3).forEach(indexDateDate -> assertUpdate(format("INSERT INTO %s(%s, %s, %s) VALUES('a-%d', 2019031%d, CAST('2020-04-%d%d' AS date))",
                            table, aCol, dateIntCol, dateDateCol, indexDateDate, indexDateInt, indexDateInt, indexDateDate),
                    1)));

            MaterializedResultWithPlan materializedResultResultWithQueryId = getQueryRunner()
                    .executeWithPlan(getSession(),
                            format("SELECT %s, %s FROM %s WHERE %s=20190311 AND %s='a-1'",
                                    aCol, dateIntCol, table, dateIntCol, aCol));

            assertThat(materializedResultResultWithQueryId.result()
                    .getStatementStats()
                    .orElseThrow()
                    .getTotalSplits())
                    .isEqualTo(2);

            assertThat(materializedResultResultWithQueryId.result().getRowCount())
                    .isEqualTo(1);

            materializedResultResultWithQueryId = getQueryRunner()
                    .executeWithPlan(getSession(),
                            format("SELECT %s, %s FROM %s WHERE %s=20190311 AND %s=CAST('2020-04-12' AS date) AND %s='a-1'",
                                    aCol, dateIntCol, table, dateIntCol, dateDateCol, aCol));

            assertThat(materializedResultResultWithQueryId.result()
                    .getStatementStats()
                    .orElseThrow()
                    .getTotalSplits())
                    .isEqualTo(1);

            assertThat(materializedResultResultWithQueryId.result().getRowCount())
                    .isEqualTo(0);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS pt");
        }
    }

    @Test
    public void testSimple()
    {
        computeActual("INSERT INTO t VALUES (1, 'shlomi')");
        MaterializedResult materializedRows = computeActual(format("SELECT %s FROM t WHERE %s = 1", C2, C1));
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
    }

    @Test
    public void test_CTAS()
    {
        computeActual("CREATE TABLE t2 AS SELECT * FROM t");
        computeActual("DROP TABLE t2");
    }

    @Test
    public void testRowDereference()
    {
        try {
            computeActual("CREATE TABLE evolve_test (dummy bigint, a row(b bigint, c varchar), d bigint)");
            computeActual("INSERT INTO evolve_test values (1, row(1, 'abc'), 1)");
            MaterializedResult materializedRows = computeActual(getSession(), "select * from evolve_test where a[1] = 1");
            assertThat(materializedRows.getRowCount()).isEqualTo(1);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS evolve_test");
        }
    }

    @Test
    public void testSimple_AnalyzeWithColumns()
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        computeActual(getSession(), "ANALYZE t WITH (columns = ARRAY['int1'])");
        assertQuery("SHOW STATS FOR t",
                "SELECT * FROM VALUES " +
                        "('int1',  null,    1,    0, null, 1, 1), " +
                        "('v1',  6,    1,    0, null, null, null), " +
                        "(null,  null,    null,   null,    1, null, null)");
    }

    @Test
    public void testWildcardCountOnDataCol()
            throws IOException
    {
        computeActual("INSERT INTO t VALUES (1, 'shlomi')");
        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        validateWarmupIsIgnored("select int1 from t");

        MaterializedResult result = computeActual("select int1 from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
        result = computeActual("select count(int1) from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);

        result = computeActual("select v1 from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo("shlomi");
        result = computeActual("select count(v1) from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);

        result = computeActual("select count(*) from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);
    }

    @Test
    public void testLikeQuery()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C2,
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));

        validateWarmupIsIgnored("select * from t");

        computeActual("select v1 from t where v1 like '%mishlomi%'");
    }

    private void validateWarmupIsIgnored(String sql)
    {
        //here we check a few times that warmup was ignored for pass-through connector
        AtomicInteger counter = new AtomicInteger(4);
        Failsafe.with(new RetryPolicy<>()
                        .withMaxRetries(10)
                        .withDelay(Duration.ofSeconds(1)))
                .run(() -> {
                    warmAndValidate(sql,
                            false,
                            0,
                            0);
                    assertThat(counter.decrementAndGet()).isZero();
                });
    }

    @Override
    @Test
    public void testGoAllProxyOnlyWhenHavePushDowns()
    {
        //Cannot warm up in pass through
    }
}
