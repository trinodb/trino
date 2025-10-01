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
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Memo;
import io.trino.sql.planner.plan.PlanNode;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.SystemSessionProperties.isEnableStatsCalculator;
import static io.trino.SystemSessionProperties.isIgnoreStatsCalculatorFailures;
import static java.util.Objects.requireNonNull;

public class CachingCostProvider
        implements CostProvider
{
    private static final Logger log = Logger.get(CachingCostProvider.class);

    private final CostCalculator costCalculator;
    private final StatsProvider statsProvider;
    private final Optional<Memo> memo;
    private final Session session;

    private final Map<PlanNode, PlanCostEstimate> cache = new IdentityHashMap<>(0);

    public CachingCostProvider(CostCalculator costCalculator, StatsProvider statsProvider, Session session)
    {
        this(costCalculator, statsProvider, Optional.empty(), session);
    }

    public CachingCostProvider(CostCalculator costCalculator, StatsProvider statsProvider, Optional<Memo> memo, Session session)
    {
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public PlanCostEstimate getCost(PlanNode node)
    {
        if (!isEnableStatsCalculator(session)) {
            return PlanCostEstimate.unknown();
        }

        requireNonNull(node, "node is null");

        try {
            if (node instanceof GroupReference group) {
                return getGroupCost(group);
            }

            PlanCostEstimate cost = cache.get(node);
            if (cost != null) {
                return cost;
            }

            cost = calculateCost(node);
            verify(cache.put(node, cost) == null, "Cost already set");
            return cost;
        }
        catch (RuntimeException e) {
            if (isIgnoreStatsCalculatorFailures(session)) {
                log.error(e, "Error occurred when computing cost for query %s", session.getQueryId());
                return PlanCostEstimate.unknown();
            }
            throw e;
        }
    }

    private PlanCostEstimate getGroupCost(GroupReference groupReference)
    {
        int group = groupReference.getGroupId();
        Memo memo = this.memo.orElseThrow(() -> new IllegalStateException("CachingCostProvider without memo cannot handle GroupReferences"));

        Optional<PlanCostEstimate> knownCost = memo.getCost(group);
        if (knownCost.isPresent()) {
            return knownCost.get();
        }

        PlanCostEstimate cost = getCost(memo.getNode(group));
        verify(memo.getCost(group).isEmpty(), "Group cost already set");
        memo.storeCost(group, cost);
        return cost;
    }

    private PlanCostEstimate calculateCost(PlanNode node)
    {
        return costCalculator.calculateCost(node, statsProvider, this, session);
    }
}
