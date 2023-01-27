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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CountingAccessMetadata;
import io.trino.metadata.MetadataManager;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true) // counting metadata is a shared mutable state
public class TestGetTableStatisticsOperations
        extends AbstractTestQueryFramework
{
    private LocalQueryRunner localQueryRunner;
    private CountingAccessMetadata metadata;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        localQueryRunner = LocalQueryRunner.builder(testSessionBuilder().build())
                .withMetadataProvider((systemSecurityMetadata, transactionManager, globalFunctionCatalog, typeManager)
                        -> new CountingAccessMetadata(new MetadataManager(systemSecurityMetadata, transactionManager, globalFunctionCatalog, typeManager)))
                .build();
        metadata = (CountingAccessMetadata) localQueryRunner.getMetadata();
        localQueryRunner.installPlugin(new TpchPlugin());
        localQueryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
        return localQueryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        localQueryRunner.close();
        localQueryRunner = null;
        metadata = null;
    }

    @BeforeMethod
    public void resetCounters()
    {
        metadata.resetCounters();
    }

    @Test
    public void testTwoWayJoin()
    {
        planDistributedQuery("SELECT * " +
                "FROM tpch.tiny.orders o, tpch.tiny.lineitem l " +
                "WHERE o.orderkey = l.orderkey");
        assertThat(metadata.getMethodInvocations()).containsExactlyInAnyOrderElementsOf(
                ImmutableMultiset.<CountingAccessMetadata.Methods>builder()
                        .addCopies(CountingAccessMetadata.Methods.GET_TABLE_STATISTICS, 2)
                        .build());
    }

    @Test
    public void testThreeWayJoin()
    {
        planDistributedQuery("SELECT * " +
                "FROM tpch.tiny.customer c, tpch.tiny.orders o, tpch.tiny.lineitem l " +
                "WHERE o.orderkey = l.orderkey AND c.custkey = o.custkey");
        assertThat(metadata.getMethodInvocations()).containsExactlyInAnyOrderElementsOf(
                ImmutableMultiset.<CountingAccessMetadata.Methods>builder()
                        .addCopies(CountingAccessMetadata.Methods.GET_TABLE_STATISTICS, 3)
                        .build());
    }

    private void planDistributedQuery(@Language("SQL") String sql)
    {
        transaction(localQueryRunner.getTransactionManager(), localQueryRunner.getAccessControl())
                .execute(localQueryRunner.getDefaultSession(), session -> {
                    localQueryRunner.createPlan(session, sql, OPTIMIZED_AND_VALIDATED, false, WarningCollector.NOOP);
                });
    }
}
