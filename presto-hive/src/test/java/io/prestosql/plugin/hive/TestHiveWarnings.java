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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.QueryId;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.airlift.tpch.TpchTable.ORDERS;
import static io.prestosql.plugin.hive.HiveQueryRunner.createQueryRunner;
import static io.prestosql.plugin.hive.HiveWarningCode.PREFERRED_FILE_FORMAT_SUGGESTION;
import static io.prestosql.plugin.hive.HiveWarningCode.TOO_MANY_PARTITIONS;
import static org.testng.Assert.assertTrue;

public class TestHiveWarnings
        extends AbstractTestQueryFramework
{
    private DistributedQueryRunner adjustedHivePropertiesQueryRunner;

    @SuppressWarnings("unused")
    public TestHiveWarnings() throws Exception
    {
        this(() -> createQueryRunner(ORDERS));
        adjustedHivePropertiesQueryRunner = createQueryRunner(
                ImmutableList.of(ORDERS),
                ImmutableMap.of(), "sql-standard",
                ImmutableMap.of("hive.preferred-file-format", "AVRO", "hive.too-many-partitions-limit", "50"),
                Optional.empty());
    }

    private TestHiveWarnings(QueryRunnerSupplier queryRunnerSupplier)
    {
        super(queryRunnerSupplier);
    }

    @Test
    public void testFormatWarning()
    {
        computeActual("CREATE TABLE test_orders WITH ( format = 'AVRO', partitioned_by = ARRAY['orderkey'] ) AS select custkey, orderkey FROM orders where orderkey < 300");
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) getQueryRunner();
        QueryId queryId = distributedQueryRunner.executeWithQueryId(getSession(), "SELECT * FROM test_orders").getQueryId();
        List<PrestoWarning> warnings = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getWarnings();
        assertWarning(warnings, ImmutableList.of(new PrestoWarning(PREFERRED_FILE_FORMAT_SUGGESTION, "Try using ORC for hive table test_orders")));

        computeActual("CREATE TABLE test_Format2 WITH ( format = 'ORC', partitioned_by = ARRAY['orderkey'] ) AS select custkey, orderkey FROM orders where orderkey < 300");
        QueryId queryId2 = distributedQueryRunner.executeWithQueryId(getSession(), "SELECT * FROM test_Format2").getQueryId();
        List<PrestoWarning> warnings2 = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId2).getWarnings();
        assertWarning(warnings2, ImmutableList.of());

        QueryId queryId3 = distributedQueryRunner.executeWithQueryId(getSession(), "SELECT * FROM (SELECT * FROM test_orders) CROSS JOIN (SELECT * FROM test_orders)").getQueryId();
        List<PrestoWarning> noDuplicateWarnings = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId3).getWarnings();
        assertWarning(noDuplicateWarnings, ImmutableList.of(new PrestoWarning(PREFERRED_FILE_FORMAT_SUGGESTION, "Try using ORC for hive table test_orders")));

        computeActual("CREATE TABLE test_orders2 WITH ( format = 'AVRO', partitioned_by = ARRAY['orderkey'] ) AS select custkey, orderkey FROM orders where orderkey < 300");

        QueryId queryId4 = distributedQueryRunner.executeWithQueryId(getSession(), "SELECT * FROM (SELECT * FROM test_orders) CROSS JOIN (SELECT * FROM test_orders2)").getQueryId();
        List<PrestoWarning> multipleWarnings = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId4).getWarnings();
        assertWarning(multipleWarnings, ImmutableList.of(
                new PrestoWarning(PREFERRED_FILE_FORMAT_SUGGESTION, "Try using ORC for hive table test_orders"),
                new PrestoWarning(PREFERRED_FILE_FORMAT_SUGGESTION, "Try using ORC for hive table test_orders2")));
    }

    @Test
    public void testAdjustedFormatWarning()
    {
        adjustedHivePropertiesQueryRunner.execute("CREATE TABLE test_orders WITH ( format = 'AVRO', partitioned_by = ARRAY['orderkey'] ) AS select custkey, orderkey FROM orders where orderkey < 10");

        QueryId queryId = adjustedHivePropertiesQueryRunner.executeWithQueryId(getSession(), "SELECT * FROM test_orders").getQueryId();
        List<PrestoWarning> warnings = adjustedHivePropertiesQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getWarnings();
        assertWarning(warnings, ImmutableList.of());
    }

    @Test
    public void testPartitionWarning()
    {
        computeActual("CREATE TABLE test_Partition WITH ( format = 'ORC', partitioned_by = ARRAY['orderkey'] ) AS select custkey, orderkey FROM orders where orderkey < 300");
        int numberOfPartitions = 305;
        while (numberOfPartitions > 0) {
            computeActual(String.format("INSERT INTO test_Partition VALUES (%s,%s)", Integer.toString(numberOfPartitions), Integer.toString(numberOfPartitions)));
            numberOfPartitions -= 1;
        }
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) getQueryRunner();
        QueryId queryId = distributedQueryRunner.executeWithQueryId(getSession(), "SELECT * FROM test_Partition").getQueryId();
        List<PrestoWarning> warnings = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getWarnings();
        assertWarning(warnings, ImmutableList.of(new PrestoWarning(TOO_MANY_PARTITIONS, "Using more than 300 partitions for table test_partition")));

        computeActual("CREATE TABLE test_Partition2 WITH ( format = 'ORC', partitioned_by = ARRAY['orderkey'] ) AS select custkey, orderkey FROM orders where orderkey < 3");
        QueryId queryId2 = distributedQueryRunner.executeWithQueryId(getSession(), "SELECT * FROM test_Partition2").getQueryId();
        List<PrestoWarning> warnings2 = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId2).getWarnings();
        assertWarning(warnings2, ImmutableList.of());

        QueryId queryId3 = distributedQueryRunner.executeWithQueryId(getSession(), "SELECT * FROM test_Partition where orderkey >= (SELECT MIN (orderkey) FROM test_Partition2)").getQueryId();
        List<PrestoWarning> warnings3 = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId3).getWarnings();
        assertWarning(warnings3, ImmutableList.of(new PrestoWarning(TOO_MANY_PARTITIONS, "Using more than 300 partitions for table test_partition")));

        QueryId queryId4 = distributedQueryRunner.executeWithQueryId(getSession(), "SELECT * FROM test_Partition WHERE orderkey BETWEEN 1 AND 75").getQueryId();
        List<PrestoWarning> warning4 = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId4).getWarnings();
        assertWarning(warning4, ImmutableList.of());
    }

    @Test
    public void testAdjustedPartitionsWarning()
    {
        adjustedHivePropertiesQueryRunner.execute("CREATE TABLE test_Partition WITH ( format = 'AVRO', partitioned_by = ARRAY['orderkey'] ) AS select custkey, orderkey FROM orders where orderkey < 300");
        int numberOfPartitions = 305;
        while (numberOfPartitions > 0) {
            adjustedHivePropertiesQueryRunner.execute(String.format("INSERT INTO test_Partition VALUES (%s,%s)", Integer.toString(numberOfPartitions), Integer.toString(numberOfPartitions)));
            numberOfPartitions -= 1;
        }

        QueryId queryId = adjustedHivePropertiesQueryRunner.executeWithQueryId(getSession(), "SELECT * FROM test_Partition WHERE orderkey BETWEEN 1 AND 75").getQueryId();
        List<PrestoWarning> warnings = adjustedHivePropertiesQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getWarnings();
        assertWarning(warnings, ImmutableList.of(new PrestoWarning(TOO_MANY_PARTITIONS, "Using more than 50 partitions for table test_partition")));
    }

    private void assertWarning(List<PrestoWarning> warningList, List<PrestoWarning> expectedWarnings)
    {
        assertTrue(warningList.size() == expectedWarnings.size());
        for (PrestoWarning warning : warningList) {
            assertTrue(expectedWarnings.contains(warning));
        }
    }
}
