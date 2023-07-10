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

import io.trino.Session;
import io.trino.cost.ComposableStatsCalculator.Rule;
import io.trino.metadata.Metadata;
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.transaction.TestingTransactionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.util.Objects.requireNonNull;

public class StatsCalculatorAssertion
{
    private final Metadata metadata;
    private final StatsCalculator statsCalculator;
    private final Session session;
    private final PlanNode planNode;
    private final TypeProvider types;

    private final Map<PlanNode, PlanNodeStatsEstimate> sourcesStats;

    private Optional<TableStatsProvider> tableStatsProvider = Optional.empty();

    public StatsCalculatorAssertion(Metadata metadata, StatsCalculator statsCalculator, Session session, PlanNode planNode, TypeProvider types)
    {
        this.metadata = requireNonNull(metadata, "metadata cannot be null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator cannot be null");
        this.session = requireNonNull(session, "session cannot be null");
        this.planNode = requireNonNull(planNode, "planNode is null");
        this.types = requireNonNull(types, "types is null");

        sourcesStats = new HashMap<>();
        planNode.getSources().forEach(child -> sourcesStats.put(child, PlanNodeStatsEstimate.unknown()));
    }

    public StatsCalculatorAssertion withSourceStats(PlanNodeStatsEstimate sourceStats)
    {
        checkState(planNode.getSources().size() == 1, "expected single source");
        return withSourceStats(0, sourceStats);
    }

    public StatsCalculatorAssertion withSourceStats(int sourceIndex, PlanNodeStatsEstimate sourceStats)
    {
        checkArgument(sourceIndex < planNode.getSources().size(), "invalid sourceIndex %s; planNode has %s sources", sourceIndex, planNode.getSources().size());
        sourcesStats.put(planNode.getSources().get(sourceIndex), sourceStats);
        return this;
    }

    public StatsCalculatorAssertion withSourceStats(PlanNodeId planNodeId, PlanNodeStatsEstimate sourceStats)
    {
        PlanNode sourceNode = PlanNodeSearcher.searchFrom(planNode).where(node -> node.getId().equals(planNodeId)).findOnlyElement();
        sourcesStats.put(sourceNode, sourceStats);
        return this;
    }

    public StatsCalculatorAssertion withSourceStats(Map<PlanNode, PlanNodeStatsEstimate> stats)
    {
        sourcesStats.putAll(stats);
        return this;
    }

    public StatsCalculatorAssertion withTableStatisticsProvider(TableStatsProvider tableStatsProvider)
    {
        this.tableStatsProvider = Optional.of(tableStatsProvider);
        return this;
    }

    public StatsCalculatorAssertion check(Consumer<PlanNodeStatsAssertion> statisticsAssertionConsumer)
    {
        PlanNodeStatsEstimate statsEstimate = transaction(new TestingTransactionManager(), new AllowAllAccessControl())
                .execute(session, transactionSession -> {
                    return statsCalculator.calculateStats(
                            planNode,
                            this::getSourceStats,
                            noLookup(),
                            transactionSession,
                            types,
                            tableStatsProvider.orElseGet(() -> new CachingTableStatsProvider(metadata, session)));
                });
        statisticsAssertionConsumer.accept(PlanNodeStatsAssertion.assertThat(statsEstimate));
        return this;
    }

    public StatsCalculatorAssertion check(Rule<?> rule, Consumer<PlanNodeStatsAssertion> statisticsAssertionConsumer)
    {
        Optional<PlanNodeStatsEstimate> statsEstimate = calculatedStats(
                rule,
                planNode,
                this::getSourceStats,
                noLookup(),
                session,
                types,
                tableStatsProvider.orElseGet(() -> new CachingTableStatsProvider(metadata, session)));
        checkState(statsEstimate.isPresent(), "Expected stats estimates to be present");
        statisticsAssertionConsumer.accept(PlanNodeStatsAssertion.assertThat(statsEstimate.get()));
        return this;
    }

    @SuppressWarnings("unchecked")
    private static <T extends PlanNode> Optional<PlanNodeStatsEstimate> calculatedStats(Rule<T> rule, PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider)
    {
        return rule.calculate((T) node, sourceStats, lookup, session, types, tableStatsProvider);
    }

    private PlanNodeStatsEstimate getSourceStats(PlanNode sourceNode)
    {
        checkArgument(sourcesStats.containsKey(sourceNode), "stats not found for source %s", sourceNode);
        return sourcesStats.get(sourceNode);
    }
}
