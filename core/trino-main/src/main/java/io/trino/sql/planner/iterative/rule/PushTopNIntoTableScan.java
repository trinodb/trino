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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortItem;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.iterative.rule.Rules.deriveTableStatisticsForPushdown;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static io.trino.sql.planner.plan.Patterns.topN;

public class PushTopNIntoTableScan
        implements Rule<TopNNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

    // Rule is executed in two planning phases. Initially we try to pushdown SINGLE TopN into
    // table scan. If that fails, we repeat the exercise for PARTIAL and FINAL TopN nodes after SINGLE -> PARTIAL/FINAL split.
    // In case of TopN over outer join, TopN may become eligible for push down only after PARTIAL TopN was pushed down
    // and only then the join was pushed down as well -- the connector may decide to accept Join pushdown only after it learns
    // there is TopN in play which limits results size.
    private static final Pattern<TopNNode> PATTERN = topN()
            .with(source().matching(tableScan().capturedAs(TABLE_SCAN)));

    private final Metadata metadata;

    public PushTopNIntoTableScan(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    @Override
    public Result apply(TopNNode topNNode, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        long topNCount = topNNode.getCount();
        List<SortItem> sortItems = topNNode.getOrderingScheme().toSortItems();

        Map<String, ColumnHandle> assignments = tableScan.getAssignments()
                .entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue));

        return metadata.applyTopN(context.getSession(), tableScan.getTable(), topNCount, sortItems, assignments)
                .map(result -> {
                    PlanNode node = new TableScanNode(
                            context.getIdAllocator().getNextId(),
                            result.getHandle(),
                            tableScan.getOutputSymbols(),
                            tableScan.getAssignments(),
                            TupleDomain.all(),
                            deriveTableStatisticsForPushdown(
                                    context.getStatsProvider(),
                                    context.getSession(),
                                    result.isPrecalculateStatistics(),
                                    result.isTopNGuaranteed() ? topNNode : tableScan),
                            tableScan.isUpdateTarget(),
                            // table scan partitioning might have changed with new table handle
                            Optional.empty());

                    // If possible we are getting rid of TopN node.
                    //
                    // If we are operating in `SINGLE` step and connector
                    // TopN pushdown is guaranteed we are removing TopN node from plan altogether.

                    // For PARTIAL step it would be semantically correct to always drop TopN node from the plan, no matter if connector
                    // declares pushdown as guaranteed or not. But we decided to leave it in the plan for non-guaranteed pushdown, as there is no way
                    // to determine the size of output returned by connector. If connector pushdown support is very limited, and still a lot of data is returned
                    // after pushdown, removing PARTIAL TopN node would make query execution significantly more expensive.
                    //
                    // If we push down FINAL step of TopN node, it means its corresponding PARTIAL was already pushed down, so it is the same
                    // case as if the TopN was pushed down as a SINGLE phase.

                    if (!result.isTopNGuaranteed()) {
                        node = topNNode.replaceChildren(ImmutableList.of(node));
                    }
                    return Result.ofPlanNode(node);
                })
                .orElseGet(Result::empty);
    }
}
