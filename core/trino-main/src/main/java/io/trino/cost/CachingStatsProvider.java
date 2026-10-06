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

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Memo;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.isEnableStatsCalculator;
import static io.trino.SystemSessionProperties.isIgnoreStatsCalculatorFailures;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static java.util.Objects.requireNonNull;

public final class CachingStatsProvider
        implements StatsProvider
{
    private static final Logger log = Logger.get(CachingStatsProvider.class);

    private final StatsCalculator statsCalculator;
    private final Optional<Memo> memo;
    private final Lookup lookup;
    private final Session session;
    private final TableStatsProvider tableStatsProvider;
    private final RuntimeInfoProvider runtimeInfoProvider;

    private final Map<PlanNode, PlanNodeStatsEstimate> cache = new IdentityHashMap<>(0);
    // JoinStatsRule reads only join type, children, criteria and filter, so copies of a join node
    // that differ only in the other fields can share an estimate. The identity cache above misses
    // on each copy the join enumerator builds, so those fall back to this lookup.
    private final Map<JoinStatsKey, PlanNodeStatsEstimate> joinCache = new HashMap<>(0);

    public CachingStatsProvider(StatsCalculator statsCalculator, Session session, TableStatsProvider tableStatsProvider)
    {
        this(statsCalculator, Optional.empty(), noLookup(), session, tableStatsProvider, RuntimeInfoProvider.noImplementation());
    }

    public CachingStatsProvider(
            StatsCalculator statsCalculator,
            Optional<Memo> memo,
            Lookup lookup,
            Session session,
            TableStatsProvider tableStatsProvider,
            RuntimeInfoProvider runtimeInfoProvider)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.session = requireNonNull(session, "session is null");
        this.tableStatsProvider = requireNonNull(tableStatsProvider, "tableStatsProvider is null");
        this.runtimeInfoProvider = requireNonNull(runtimeInfoProvider, "runtimeInfoProvider is null");
    }

    @Override
    public PlanNodeStatsEstimate getStats(PlanNode node)
    {
        if (!isEnableStatsCalculator(session)) {
            return PlanNodeStatsEstimate.unknown();
        }

        requireNonNull(node, "node is null");

        try {
            if (node instanceof GroupReference group) {
                return getGroupStats(group);
            }

            PlanNodeStatsEstimate stats = cache.get(node);
            if (stats != null) {
                return stats;
            }

            stats = node instanceof JoinNode joinNode
                    ? getJoinStats(joinNode)
                    : statsCalculator.calculateStats(node, new StatsCalculator.Context(this, lookup, session, tableStatsProvider, runtimeInfoProvider));
            verify(cache.put(node, stats) == null, "Stats already set");
            return stats;
        }
        catch (RuntimeException e) {
            if (isIgnoreStatsCalculatorFailures(session)) {
                log.warn(e, "Error occurred when computing stats for query %s", session.getQueryId());
                return PlanNodeStatsEstimate.unknown();
            }
            throw e;
        }
    }

    private PlanNodeStatsEstimate getJoinStats(JoinNode node)
    {
        JoinStatsKey key = JoinStatsKey.of(node);
        PlanNodeStatsEstimate stats = joinCache.get(key);
        if (stats == null && node.getType() == INNER) {
            stats = joinCache.get(key.flipped());
        }
        if (stats == null) {
            stats = statsCalculator.calculateStats(node, new StatsCalculator.Context(this, lookup, session, tableStatsProvider, runtimeInfoProvider));
            joinCache.put(key, stats);
        }
        return stats;
    }

    // PlanNode does not override equals, so left and right compare by identity
    private record JoinStatsKey(JoinType type, PlanNode left, PlanNode right, List<EquiJoinClause> criteria, Optional<Expression> filter)
    {
        static JoinStatsKey of(JoinNode node)
        {
            return new JoinStatsKey(node.getType(), node.getLeft(), node.getRight(), node.getCriteria(), node.getFilter());
        }

        JoinStatsKey flipped()
        {
            return new JoinStatsKey(
                    type,
                    /* left= */ right,
                    /* right= */ left,
                    criteria.stream().map(EquiJoinClause::flip).collect(toImmutableList()),
                    filter);
        }
    }

    private PlanNodeStatsEstimate getGroupStats(GroupReference groupReference)
    {
        int group = groupReference.getGroupId();
        Memo memo = this.memo.orElseThrow(() -> new IllegalStateException("CachingStatsProvider without memo cannot handle GroupReferences"));

        Optional<PlanNodeStatsEstimate> stats = memo.getStats(group);
        if (stats.isPresent()) {
            return stats.get();
        }

        PlanNodeStatsEstimate groupStats = getStats(memo.getNode(group));
        verify(memo.getStats(group).isEmpty(), "Group stats already set");
        memo.storeStats(group, groupStats);
        return groupStats;
    }
}
