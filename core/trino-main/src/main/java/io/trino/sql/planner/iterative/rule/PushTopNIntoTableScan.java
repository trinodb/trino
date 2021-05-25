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

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortItem;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static io.trino.sql.planner.plan.Patterns.topN;

public class PushTopNIntoTableScan
        implements Rule<TopNNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

    private static final Pattern<TopNNode> PATTERN = topN().with(source().matching(
            tableScan().capturedAs(TABLE_SCAN)));

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
                    PlanNode node = TableScanNode.newInstance(
                            context.getIdAllocator().getNextId(),
                            result.getHandle(),
                            tableScan.getOutputSymbols(),
                            tableScan.getAssignments(),
                            tableScan.isForDelete());

                    if (!result.isTopNGuaranteed()) {
                        node = new TopNNode(topNNode.getId(), node, topNNode.getCount(), topNNode.getOrderingScheme(), TopNNode.Step.FINAL);
                    }
                    return Result.ofPlanNode(node);
                })
                .orElseGet(Result::empty);
    }
}
