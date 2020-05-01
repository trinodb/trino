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
package io.prestosql.tests;

import io.prestosql.Session;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManager;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.ResultWithQueryId;
import io.prestosql.tests.tpch.TpchQueryRunnerBuilder;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.LATE_MATERIALIZATION;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.prestosql.testing.QueryAssertions.assertEqualsIgnoreOrder;
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
        assertLazyQuery("SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM orders WHERE orderdate >= DATE '1994-01-01')");
    }

    @Test
    public void testJoin()
    {
        assertLazyQuery("SELECT * FROM lineitem INNER JOIN part ON lineitem.partkey = part.partkey AND mfgr = 'Manufacturer#5'");
    }

    private void assertLazyQuery(@Language("SQL") String sql)
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();

        ResultWithQueryId<MaterializedResult> workProcessorResultResultWithQueryId = queryRunner.executeWithQueryId(lateMaterialization(), sql);
        QueryInfo workProcessorQueryInfo = queryManager.getFullQueryInfo(workProcessorResultResultWithQueryId.getQueryId());

        ResultWithQueryId<MaterializedResult> noWorkProcessorResultResultWithQueryId = queryRunner.executeWithQueryId(noLateMaterialization(), sql);
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
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(LATE_MATERIALIZATION, "true")
                .build();
    }

    private Session noLateMaterialization()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(LATE_MATERIALIZATION, "false")
                .build();
    }
}
