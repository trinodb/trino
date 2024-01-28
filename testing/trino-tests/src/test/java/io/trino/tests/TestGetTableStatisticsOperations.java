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
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.testing.TestingSession.testSession;
import static io.trino.testing.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestGetTableStatisticsOperations
        extends AbstractTestQueryFramework
{
    private QueryRunner queryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        queryRunner = new StandaloneQueryRunner(testSession());
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
        return queryRunner;
    }

    @AfterAll
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testTwoWayJoin()
    {
        planDistributedQuery("SELECT * " +
                "FROM tpch.tiny.orders o, tpch.tiny.lineitem l " +
                "WHERE o.orderkey = l.orderkey");
        assertThat(getTableStatisticsMethodInvocations()).isEqualTo(2);
    }

    @Test
    public void testThreeWayJoin()
    {
        planDistributedQuery("SELECT * " +
                "FROM tpch.tiny.customer c, tpch.tiny.orders o, tpch.tiny.lineitem l " +
                "WHERE o.orderkey = l.orderkey AND c.custkey = o.custkey");
        assertThat(getTableStatisticsMethodInvocations()).isEqualTo(3);
    }

    private void planDistributedQuery(@Language("SQL") String sql)
    {
        transaction(queryRunner.getTransactionManager(), queryRunner.getPlannerContext().getMetadata(), queryRunner.getAccessControl())
                .execute(queryRunner.getDefaultSession(), transactionSession -> {
                    queryRunner.createPlan(transactionSession, sql);
                });
    }

    private long getTableStatisticsMethodInvocations()
    {
        return queryRunner.getSpans().stream()
                .map(SpanData::getName)
                .filter(name -> name.equals("Metadata.getTableStatistics"))
                .count();
    }
}
