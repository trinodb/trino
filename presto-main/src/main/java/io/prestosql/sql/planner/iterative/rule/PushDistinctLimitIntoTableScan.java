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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.DistinctLimitNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.PushAggregationIntoTableScan.pushAggregationIntoTableScan;
import static io.prestosql.sql.planner.plan.Patterns.distinctLimit;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

public class PushDistinctLimitIntoTableScan
        implements Rule<DistinctLimitNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

    private static final Pattern<DistinctLimitNode> PATTERN =
            distinctLimit()
                    .with(source().matching(tableScan().capturedAs(TABLE_SCAN)));

    private final Metadata metadata;

    public PushDistinctLimitIntoTableScan(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
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
                metadata,
                context,
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
