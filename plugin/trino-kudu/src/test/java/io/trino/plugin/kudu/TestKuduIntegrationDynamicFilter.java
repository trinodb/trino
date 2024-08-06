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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableSet;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.split.SplitSource;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKuduIntegrationDynamicFilter
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return KuduQueryRunnerFactory.builder(closeAfterClass(new TestingKuduServer()))
                .setKuduSchemaEmulationPrefix(Optional.of(""))
                .addConnectorProperty("kudu.dynamic-filtering.wait-timeout", "1h")
                .addExtraProperty("dynamic-filtering.small.max-distinct-values-per-driver", "100")
                .addExtraProperty("dynamic-filtering.small.range-row-limit-per-driver", "100")
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
                .setCatalogSessionProperty("kudu", "dynamic_filtering_wait_timeout", "1s")
                .build()
                .beginTransactionId(transactionId, transactionManager, new AllowAllAccessControl());
        QualifiedObjectName tableName = new QualifiedObjectName("kudu", "tpch", "orders");
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

    private void assertDynamicFiltering(@Language("SQL") String selectQuery, Session session, int expectedRowCount, int expectedProbeInputRowsRead)
    {
        QueryRunner runner = getDistributedQueryRunner();
        MaterializedResultWithPlan result = runner.executeWithPlan(session, selectQuery);

        assertThat(result.result().getRowCount()).isEqualTo(expectedRowCount);
        assertThat(getScanInputRowsRead(result.queryId())).isEqualTo(expectedProbeInputRowsRead);
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

    private long getScanInputRowsRead(QueryId queryId)
    {
        Plan plan = getDistributedQueryRunner().getQueryPlan(queryId);
        FilterNode planNode = (FilterNode) PlanNodeSearcher.searchFrom(plan.getRoot())
                .where(node -> {
                    if (!(node instanceof FilterNode filterNode)) {
                        return false;
                    }
                    if (!(filterNode.getSource() instanceof TableScanNode tableScanNode)) {
                        return false;
                    }
                    if (extractDynamicFilters(filterNode.getPredicate()).getDynamicConjuncts().isEmpty()) {
                        return false;
                    }
                    return ((KuduTableHandle) tableScanNode.getTable().connectorHandle())
                            .getSchemaTableName().equals(new SchemaTableName("tpch", "lineitem"));
                })
                .findOnlyElement();
        return extractOperatorStatsForNodeId(queryId, planNode.getId(), "ScanFilterAndProjectOperator").getInputPositions();
    }
}
