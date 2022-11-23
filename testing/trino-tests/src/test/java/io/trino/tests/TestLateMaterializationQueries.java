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
package io.trino.tests;

import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.LATE_MATERIALIZATION;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertTrue;

public class TestLateMaterializationQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder
                .builder()
                .amendSession(builder -> builder
                        .setSystemProperty(LATE_MATERIALIZATION, "true")
                        // disable semi join to inner join rewrite to test semi join operators explicitly
                        .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                        // make sure synthetic order (with effective filtering) is used in tests
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.toString())
                        .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.toString()))
                // make TPCH connector produce multiple pages per split
                .withProducePages(true)
                .withMaxRowsPerPage(10)
                .build();
    }

    @Test
    public void testTopN()
    {
        assertLazyQuery("SELECT * FROM orders ORDER BY totalprice LIMIT 10");
        assertLazyQuery("SELECT * FROM orders WHERE orderkey >= 10 ORDER BY totalprice LIMIT 10");
    }

    @Test
    public void testSemiJoin()
    {
        assertLazyQuery("SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderdate < DATE '1994-01-01')");
        assertLazyQuery("" +
                "SELECT * " +
                "FROM (SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM orders WHERE orderdate > DATE '2200-01-01')) " +
                "WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderdate < DATE '1994-01-01')");
        assertLazyQuery("SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM orders WHERE orderdate >= DATE '1994-01-01')");
    }

    @Test
    public void testJoin()
    {
        assertLazyQuery("SELECT * FROM lineitem INNER JOIN part ON lineitem.partkey = part.partkey AND mfgr = 'Manufacturer#5'");
        assertLazyQuery("" +
                "SELECT * " +
                "FROM (SELECT a.* FROM lineitem a INNER JOIN lineitem b ON a.partkey = b.partkey AND a.suppkey = b.suppkey AND a.orderkey = b.orderkey) c " +
                "INNER JOIN part ON c.partkey = part.partkey AND mfgr = 'Manufacturer#5'");
    }

    private void assertLazyQuery(@Language("SQL") String sql)
    {
        QueryManager queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();

        MaterializedResultWithQueryId workProcessorResultResultWithQueryId = getDistributedQueryRunner().executeWithQueryId(lateMaterialization(), sql);
        QueryInfo workProcessorQueryInfo = queryManager.getFullQueryInfo(workProcessorResultResultWithQueryId.getQueryId());

        MaterializedResultWithQueryId noWorkProcessorResultResultWithQueryId = getDistributedQueryRunner().executeWithQueryId(noLateMaterialization(), sql);
        QueryInfo noWorkProcessorQueryInfo = queryManager.getFullQueryInfo(noWorkProcessorResultResultWithQueryId.getQueryId());

        // ensure results are correct
        MaterializedResult expected = computeExpected(sql, workProcessorResultResultWithQueryId.getResult().getTypes());
        assertEqualsIgnoreOrder(workProcessorResultResultWithQueryId.getResult(), expected, "For query: \n " + sql);
        assertEqualsIgnoreOrder(noWorkProcessorResultResultWithQueryId.getResult(), expected, "For query: \n " + sql);

        // ensure work processor query processed less input data
        long workProcessorProcessedInputBytes = workProcessorQueryInfo.getQueryStats().getProcessedInputDataSize().toBytes();
        long noWorkProcessorProcessedInputBytes = noWorkProcessorQueryInfo.getQueryStats().getProcessedInputDataSize().toBytes();
        assertTrue(workProcessorProcessedInputBytes < noWorkProcessorProcessedInputBytes, "Expected work processor query to process less input data");
    }

    private Session lateMaterialization()
    {
        return Session.builder(getSession())
                .setSystemProperty(LATE_MATERIALIZATION, "true")
                .build();
    }

    private Session noLateMaterialization()
    {
        return Session.builder(getSession())
                .setSystemProperty(LATE_MATERIALIZATION, "false")
                .build();
    }
}
