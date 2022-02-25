/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.trino.cost;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.matching.Pattern;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.DistinctLimitNode;

import java.util.Optional;

import static io.trino.sql.planner.plan.Patterns.distinctLimit;
import static java.lang.Math.min;

public class DistinctLimitStatsRule
        extends SimpleStatsRule<DistinctLimitNode>
{
    private static final Pattern<DistinctLimitNode> PATTERN = distinctLimit();

    public DistinctLimitStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<DistinctLimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(DistinctLimitNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        if (node.isPartial()) {
            return Optional.empty();
        }

        PlanNodeStatsEstimate distinctStats = AggregationStatsRule.groupBy(
                statsProvider.getStats(node.getSource()),
                node.getDistinctSymbols(),
                ImmutableMap.of());
        PlanNodeStatsEstimate distinctLimitStats = distinctStats.mapOutputRowCount(rowCount -> min(rowCount, node.getLimit()));
        return Optional.of(distinctLimitStats);
    }
}
