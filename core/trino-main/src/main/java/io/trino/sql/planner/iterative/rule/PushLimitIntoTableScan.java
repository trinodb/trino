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

import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.iterative.rule.Rules.deriveTableStatisticsForPushdown;
import static io.trino.sql.planner.plan.Patterns.Limit.requiresPreSortedInputs;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;

public class PushLimitIntoTableScan
        implements Rule<LimitNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<LimitNode> PATTERN = limit()
            .matching(limit -> !limit.isWithTies())
            // Currently ConnectorMetadata#applyLimit does not handle Limit with pre-sorted inputs
            .with(requiresPreSortedInputs().equalTo(false))
            .with(source().matching(
                    tableScan().capturedAs(TABLE_SCAN)));

    private final Metadata metadata;

    public PushLimitIntoTableScan(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    @Override
    public Rule.Result apply(LimitNode limit, Captures captures, Rule.Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        return metadata.applyLimit(context.getSession(), tableScan.getTable(), limit.getCount())
                .map(result -> {
                    PlanNode node = new TableScanNode(
                            tableScan.getId(),
                            result.getHandle(),
                            tableScan.getOutputSymbols(),
                            tableScan.getAssignments(),
                            tableScan.getEnforcedConstraint(),
                            deriveTableStatisticsForPushdown(
                                    context.getStatsProvider(),
                                    context.getSession(),
                                    result.isPrecalculateStatistics(),
                                    result.isLimitGuaranteed() ? limit : tableScan),
                            tableScan.isUpdateTarget(),
                            // table scan partitioning might have changed with new table handle
                            Optional.empty());

                    if (!result.isLimitGuaranteed()) {
                        node = new LimitNode(limit.getId(), node, limit.getCount(), limit.isPartial());
                    }

                    return Result.ofPlanNode(node);
                })
                .orElseGet(Result::empty);
    }
}
