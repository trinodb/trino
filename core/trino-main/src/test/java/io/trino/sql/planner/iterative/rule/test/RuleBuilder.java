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
package io.trino.sql.planner.iterative.rule.test;

import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsProvider;
import io.trino.cost.TableStatsProvider;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.LocalQueryRunner;
import io.trino.transaction.TransactionId;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.testing.TestingSession.testSession;
import static java.util.Objects.requireNonNull;

public class RuleBuilder
{
    private final Rule<?> rule;
    private final LocalQueryRunner queryRunner;
    private Session session;

    private final TestingStatsCalculator statsCalculator;

    RuleBuilder(Rule<?> rule, LocalQueryRunner queryRunner, Session session)
    {
        this.rule = requireNonNull(rule, "rule is null");
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
        this.session = requireNonNull(session, "session is null");

        this.statsCalculator = new TestingStatsCalculator(queryRunner.getStatsCalculator());
    }

    public RuleBuilder setSystemProperty(String key, String value)
    {
        return withSession(Session.builder(session)
                .setSystemProperty(key, value)
                .build());
    }

    public RuleBuilder withSession(Session session)
    {
        this.session = session;
        return this;
    }

    public RuleBuilder overrideStats(String nodeId, PlanNodeStatsEstimate nodeStats)
    {
        statsCalculator.setNodeStats(new PlanNodeId(nodeId), nodeStats);
        return this;
    }

    public RuleAssert on(Function<PlanBuilder, PlanNode> planProvider)
    {
        // Generate a new random queryId in case the rule cleanup code is not executed
        Session session = testSession(this.session);
        // start a transaction to allow catalog access
        TransactionId transactionId = queryRunner.getTransactionManager().beginTransaction(READ_UNCOMMITTED, false, false);
        Session transactionSession = session.beginTransactionId(transactionId, queryRunner.getTransactionManager(), queryRunner.getAccessControl());
        queryRunner.getMetadata().beginQuery(transactionSession);
        try {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            transactionSession.getCatalog().ifPresent(catalog -> queryRunner.getMetadata().getCatalogHandle(transactionSession, catalog));

            PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
            PlanBuilder builder = new PlanBuilder(idAllocator, queryRunner.getPlannerContext(), transactionSession);
            PlanNode plan = planProvider.apply(builder);
            TypeProvider types = builder.getTypes();
            return new RuleAssert(rule, queryRunner, statsCalculator, transactionSession, idAllocator, plan, types);
        }
        catch (Throwable t) {
            queryRunner.getMetadata().cleanupQuery(session);
            queryRunner.getTransactionManager().asyncAbort(transactionId);
            throw t;
        }
    }

    private static class TestingStatsCalculator
            implements StatsCalculator
    {
        private final StatsCalculator delegate;
        private final Map<PlanNodeId, PlanNodeStatsEstimate> stats = new HashMap<>();

        TestingStatsCalculator(StatsCalculator delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider)
        {
            if (stats.containsKey(node.getId())) {
                return stats.get(node.getId());
            }
            return delegate.calculateStats(node, sourceStats, lookup, session, types, tableStatsProvider);
        }

        public void setNodeStats(PlanNodeId nodeId, PlanNodeStatsEstimate nodeStats)
        {
            stats.put(nodeId, nodeStats);
        }
    }
}
