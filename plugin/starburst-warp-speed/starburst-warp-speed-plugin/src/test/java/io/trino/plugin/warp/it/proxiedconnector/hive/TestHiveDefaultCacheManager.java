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
package io.trino.plugin.warp.it.proxiedconnector.hive;

import io.trino.operator.OperatorStats;
import io.trino.plugin.varada.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.configuration.WarpExtensionConfiguration.USE_HTTP_SERVER_PORT;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveDefaultCacheManager
        extends DispatcherStubsIntegrationSmokeIT
{
    public TestHiveDefaultCacheManager()
    {
        super(1, "hive1");
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
                        "hive.s3.aws-access-key", "this is a fake key",
                        WARP_SPEED_PREFIX + USE_HTTP_SERVER_PORT, Boolean.FALSE.toString(),
                        "node.environment", "varada",
                        WARP_SPEED_PREFIX + PROXIED_CONNECTOR, HIVE_CONNECTOR_NAME),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                catalog,
                new WarpPlugin(),
                Map.of("cache.enabled", "true"));
    }

    @Test
    public void testSimple_Warm()
    {
        prepare();
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();

        @Language("SQL") String query = "select int1, v1 from table1 where v1 like '%shlomi%'";
        runQueryAndValidateReadFromCache(queryRunner, query);

        @Language("SQL") String query2 = "select int1, v1 from table2 where v1 like '%shlomi%'";
        runQueryAndValidateReadFromCache(queryRunner, query2);

        DistributedQueryRunner queryRunner2 = (DistributedQueryRunner) getQueryRunner();
        @Language("SQL") String unionQuery = "select * from table1 b where b.int1 > 0 union all select * from table2";
        runQueryAndValidateReadFromCache(queryRunner2, unionQuery);
        assertExplain("explain " + unionQuery, "CacheData\\[\\]\n.*\n.*TableScan.*");
    }

    /**
     * run the same query twice - first run it should store in cache, second should read from cache
     */
    private void runQueryAndValidateReadFromCache(DistributedQueryRunner queryRunner, @Language("SQL") String query)
    {
        MaterializedResultWithPlan firstRun = queryRunner.executeWithPlan(getSession(), query);
        List<String> firstRunOperatorTypes = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(firstRun.queryId()).getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getOperatorType).collect(Collectors.toList());
        assertThat(firstRunOperatorTypes).doesNotContain("LoadCachedDataOperator");

        MaterializedResultWithPlan secondRun = queryRunner.executeWithPlan(getSession(), query);
        List<String> secondRunOperatorTypes = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(secondRun.queryId()).getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getOperatorType).collect(Collectors.toList());
        assertThat(secondRunOperatorTypes).contains("LoadCachedDataOperator");
    }

    /**
     * warm table1, table2 with default warming.
     * int_1- DATA, BASIC. v1- DATA, BASIC, LUCENE
     */
    private void prepare()
    {
        assertUpdate("CREATE TABLE table1(" +
                "int1 integer, " +
                "v1 varchar(20)) WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual(getSession(), "INSERT INTO table1 VALUES (1, 'shlomi'), (2, 'kobi')");
        assertUpdate("CREATE TABLE table2(" +
                "int1 integer, " +
                "v1 varchar(20)) WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual(getSession(), "INSERT INTO table2 VALUES (3, 'roman'), (4, 'tal')");
        String query2 = "select int1, v1 from table2 where int1 > 0 and v1 like '%shlomi%' and v1 > 's' and upper(v1) = 'SHLOMI'";
        warmAndValidate(query2, true, 5, 2);
        //warm int_1 (DATA, BASIC), v1 (DATA, BASIC, LUCENE) with default warming
        String query = "select int1, v1 from table1 where int1 > 0 and v1 like '%shlomi%' and v1 > 's' and upper(v1) = 'SHLOMI'";
        warmAndValidate(query, true, 5, 2);
    }

    @Override
    @Test
    public void testGoAllProxyOnlyWhenHavePushDowns()
    {
        //not relevant
    }
}
