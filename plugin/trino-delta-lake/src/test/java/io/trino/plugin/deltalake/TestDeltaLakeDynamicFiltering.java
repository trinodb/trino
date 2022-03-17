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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryStats;
import io.trino.operator.OperatorStats;
import io.trino.plugin.deltalake.util.DockerizedMinioDataLake;
import io.trino.spi.QueryId;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.plugin.deltalake.DeltaLakeDockerizedMinioDataLake.createDockerizedMinioDataLakeForDeltaLake;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;

public class TestDeltaLakeDynamicFiltering
        extends AbstractTestQueryFramework
{
    private static final String BUCKET_NAME = "delta-lake-test-dynamic-filtering";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verify(new DynamicFilterConfig().isEnableDynamicFiltering(), "this class assumes dynamic filtering is enabled by default");
        DockerizedMinioDataLake dockerizedMinioDataLake = closeAfterClass(createDockerizedMinioDataLakeForDeltaLake(BUCKET_NAME));
        QueryRunner queryRunner = DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                "default",
                // Slowing down the query ensures the dynamic filter has enough time to populate.
                ImmutableMap.of("delta.max-splits-per-second", "3"),
                dockerizedMinioDataLake.getMinioAddress(),
                dockerizedMinioDataLake.getTestingHadoop());

        ImmutableList.of(LINE_ITEM, ORDERS).forEach(table -> {
            String tableName = table.getTableName();
            dockerizedMinioDataLake.copyResources("io/trino/plugin/deltalake/testing/resources/databricks/" + tableName, tableName);
            queryRunner.execute(format("CREATE TABLE %s.%s.%s (dummy int) WITH (location = 's3://%s/%3$s')",
                    DELTA_CATALOG,
                    "default",
                    tableName,
                    BUCKET_NAME));
        });
        return queryRunner;
    }

    @DataProvider
    public Object[][] joinDistributionTypes()
    {
        return Stream.of(JoinDistributionType.values())
                .collect(toDataProvider());
    }

    @Test(timeOut = 60_000, dataProvider = "joinDistributionTypes")
    public void testDynamicFiltering(JoinDistributionType joinDistributionType)
    {
        String query = "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice > 59995 AND orders.totalprice < 60000";
        ResultWithQueryId<MaterializedResult> filteredResult = getDistributedQueryRunner().executeWithQueryId(sessionWithDynamicFiltering(true, joinDistributionType), query);
        ResultWithQueryId<MaterializedResult> unfilteredResult = getDistributedQueryRunner().executeWithQueryId(sessionWithDynamicFiltering(false, joinDistributionType), query);
        assertEqualsIgnoreOrder(filteredResult.getResult().getMaterializedRows(), unfilteredResult.getResult().getMaterializedRows());

        QueryInputStats filteredStats = getQueryInputStats(filteredResult.getQueryId());
        QueryInputStats unfilteredStats = getQueryInputStats(unfilteredResult.getQueryId());
        assertGreaterThan(unfilteredStats.numberOfSplits, filteredStats.numberOfSplits);
        assertGreaterThan(unfilteredStats.inputPositions, filteredStats.inputPositions);
    }

    private Session sessionWithDynamicFiltering(boolean enabled, JoinDistributionType joinDistributionType)
    {
        return Session.builder(noJoinReordering(joinDistributionType))
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, String.valueOf(enabled))
                .build();
    }

    private QueryInputStats getQueryInputStats(QueryId queryId)
    {
        QueryStats stats = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats();
        long numberOfSplits = stats.getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().equals("ScanFilterAndProjectOperator"))
                .mapToLong(OperatorStats::getTotalDrivers)
                .sum();
        long inputPositions = stats.getPhysicalInputPositions();
        return new QueryInputStats(numberOfSplits, inputPositions);
    }

    private static class QueryInputStats
    {
        final long numberOfSplits;
        final long inputPositions;

        QueryInputStats(long numberOfSplits, long inputPositions)
        {
            this.numberOfSplits = numberOfSplits;
            this.inputPositions = inputPositions;
        }
    }
}
