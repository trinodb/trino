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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.iterative.rule.PushAggregationIntoTableScan.pushAggregationIntoTableScan;
import static io.trino.sql.planner.plan.Patterns.DistinctLimit.isPartial;
import static io.trino.sql.planner.plan.Patterns.distinctLimit;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

public class PushDistinctLimitIntoTableScan
        implements Rule<DistinctLimitNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

    private static final Pattern<DistinctLimitNode> PATTERN =
            distinctLimit()
                    .with(isPartial().equalTo(false))
                    .with(source().matching(tableScan().capturedAs(TABLE_SCAN)));

    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;

    public PushDistinctLimitIntoTableScan(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<DistinctLimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    @Override
    public Result apply(DistinctLimitNode node, Captures captures, Context context)
    {
        Optional<PlanNode> result = pushAggregationIntoTableScan(
                plannerContext,
                typeAnalyzer,
                context,
                node,
                captures.get(TABLE_SCAN),
                ImmutableMap.of(),
                node.getDistinctSymbols());

        if (result.isEmpty()) {
            return Result.empty();
        }

        return Result.ofPlanNode(
                // The LimitNode retained here may be later pushed into TableScan as well by a different rule
                new LimitNode(
                        node.getId(),
                        result.get(),
                        node.getLimit(),
                        node.isPartial()));
    }
}
