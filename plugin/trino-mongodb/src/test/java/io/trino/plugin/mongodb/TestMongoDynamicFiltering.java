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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorStats;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.split.SplitSource;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.plugin.mongodb.MongoSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoDynamicFiltering
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        MongoServer server = closeAfterClass(new MongoServer());
        return MongoQueryRunner.builder(server)
                .addConnectorProperties(Map.of("mongodb.dynamic-filtering.wait-timeout", "1h"))
                .setInitialTables(List.of(LINE_ITEM, ORDERS, PART))
                .build();
    }

    @Test
    @Timeout(30)
    public void testIncompleteDynamicFilterTimeout()
            throws Exception
    {
        QueryRunner runner = getQueryRunner();
        TransactionManager transactionManager = runner.getTransactionManager();
        TransactionId transactionId = transactionManager.beginTransaction(false);
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("mongodb", DYNAMIC_FILTERING_WAIT_TIMEOUT, "1s")
                .build()
                .beginTransactionId(transactionId, transactionManager, new AllowAllAccessControl());
        QualifiedObjectName tableName = new QualifiedObjectName("mongodb", "tpch", "orders");
        Optional<TableHandle> tableHandle = runner.getPlannerContext().getMetadata().getTableHandle(session, tableName);
        assertThat(tableHandle.isPresent()).isTrue();
        CompletableFuture<Void> dynamicFilterBlocked = new CompletableFuture<>();
        try {
            SplitSource splitSource = runner.getSplitManager()
                    .getSplits(session, Span.getInvalid(), tableHandle.get(), new BlockedDynamicFilter(dynamicFilterBlocked), alwaysTrue());
            List<Split> splits = new ArrayList<>();
            while (!splitSource.isFinished()) {
                splits.addAll(splitSource.getNextBatch(1000).get().getSplits());
            }
            splitSource.close();
            assertThat(splits.isEmpty()).isFalse();
        }
        finally {
            dynamicFilterBlocked.complete(null);
        }
    }

    private static class BlockedDynamicFilter
            implements DynamicFilter
    {
        private final CompletableFuture<?> isBlocked;

        public BlockedDynamicFilter(CompletableFuture<?> isBlocked)
        {
            this.isBlocked = requireNonNull(isBlocked, "isBlocked is null");
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return ImmutableSet.of();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return isBlocked;
        }

        @Override
        public boolean isComplete()
        {
            return false;
        }

        @Override
        public boolean isAwaitable()
        {
            return true;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return TupleDomain.all();
        }
    }

    @Test
    public void testJoinDynamicFilteringSingleValue()
    {
        // Join lineitem with a single row of orders
        assertDynamicFiltering(
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment = 'nstructions sleep furiously among '",
                withBroadcastJoin(),
                6,
                6);
    }

    @Test
    public void testJoinDynamicFilteringBlockProbeSide()
    {
        // Wait for both build side to finish before starting the scan of 'lineitem' table (should be very selective given the dynamic filters).
        assertDynamicFiltering(
                "SELECT l.comment" +
                        " FROM  lineitem l, orders o" +
                        " WHERE l.orderkey = o.orderkey AND o.comment = 'nstructions sleep furiously among '",
                withBroadcastJoinNonReordering(),
                6,
                6);
    }

    private void assertDynamicFiltering(@Language("SQL") String selectQuery, Session session, int expectedRowCount, int... expectedOperatorRowsRead)
    {
        QueryRunner runner = getDistributedQueryRunner();
        QueryRunner.MaterializedResultWithPlan result = runner.executeWithPlan(session, selectQuery);

        assertThat(result.result().getRowCount()).isEqualTo(expectedRowCount);
        assertThat(getOperatorRowsRead(runner, result.queryId())).isEqualTo(Ints.asList(expectedOperatorRowsRead));
    }

    private Session withBroadcastJoin()
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
    }

    private Session withBroadcastJoinNonReordering()
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .build();
    }

    private static List<Integer> getOperatorRowsRead(QueryRunner runner, QueryId queryId)
    {
        QueryStats stats = runner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats();
        return stats.getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getDynamicFilterSplitsProcessed() > 0)
                .map(OperatorStats::getInputPositions)
                .map(Math::toIntExact)
                .collect(toImmutableList());
    }
}
