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
package io.trino.cost;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.LocalQueryRunner;
import io.trino.transaction.TransactionId;

import java.util.function.Function;

import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class StatsCalculatorTester
        implements AutoCloseable
{
    private final LocalQueryRunner queryRunner;

    public StatsCalculatorTester()
    {
        this(testSessionBuilder().build());
    }

    public StatsCalculatorTester(Session session)
    {
        this(createQueryRunner(session));
    }

    private StatsCalculatorTester(LocalQueryRunner queryRunner)
    {
        this.queryRunner = queryRunner;
    }

    private static LocalQueryRunner createQueryRunner(Session session)
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);
        queryRunner.createCatalog(session.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
        return queryRunner;
    }

    public StatsCalculatorAssertion assertStatsFor(Function<PlanBuilder, PlanNode> planProvider)
    {
        return assertStatsFor(queryRunner.getDefaultSession(), planProvider);
    }

    public StatsCalculatorAssertion assertStatsFor(Session session, Function<PlanBuilder, PlanNode> planProvider)
    {
        // Unlike RuleTester, this class uses multiple final check statements, so there is no way to actually clean up transactions.
        // Generate a new query id for each test to avoid collisions due to the leak
        session = testSession(session);
        // start a transaction to allow catalog access
        TransactionId transactionId = queryRunner.getTransactionManager().beginTransaction(READ_UNCOMMITTED, false, false);
        Session transactionSession = session.beginTransactionId(transactionId, queryRunner.getTransactionManager(), queryRunner.getAccessControl());
        queryRunner.getMetadata().beginQuery(transactionSession);
        try {
            PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), queryRunner.getPlannerContext(), transactionSession);
            PlanNode planNode = planProvider.apply(planBuilder);
            return new StatsCalculatorAssertion(queryRunner, transactionSession, planNode, planBuilder.getTypes());
        }
        catch (Throwable t) {
            queryRunner.getTransactionManager().asyncAbort(transactionId);
            throw t;
        }
    }

    @Override
    public void close()
    {
        queryRunner.close();
    }
}
