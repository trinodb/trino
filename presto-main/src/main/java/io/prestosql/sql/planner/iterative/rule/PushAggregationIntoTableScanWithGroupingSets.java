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

import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.GroupIdNode;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.matching.Pattern.typeOf;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static java.util.function.Function.identity;

public class PushAggregationIntoTableScanWithGroupingSets
        implements Rule<AggregationNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Capture<GroupIdNode> GROUP_ID = newCapture();
    private static final Pattern<AggregationNode> PATTERN =
            aggregation()
                    .matching(PushAggregationIntoTableScan::allArgumentsAreSimpleReferences)
                    .matching(PushAggregationIntoTableScan::hasNoMasks)
                    .with(source().matching(typeOf(GroupIdNode.class).capturedAs(GROUP_ID)
                            .with(source().matching(tableScan().capturedAs(TABLE_SCAN)))));

    private final PushAggregationIntoTableScan pushAggregationIntoTableScan;

    public PushAggregationIntoTableScanWithGroupingSets(Metadata metadata)
    {
        this.pushAggregationIntoTableScan = new PushAggregationIntoTableScan(metadata);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        GroupIdNode groupIdNode = captures.get(GROUP_ID);

        final List<List<Symbol>> groupBySymbols = groupIdNode.getGroupingSets().stream()
                .map(groupIdNodeSymbols -> groupIdNodeSymbols.stream()
                        .map(groupIdNode.getGroupingColumns()::get)
                        .collect(toImmutableList()))
                .collect(toImmutableList());

        final Map<Symbol, Symbol> groupingSetSymbolMapping = groupIdNode.getGroupingSets().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(identity(), groupIdNode.getGroupingColumns()::get, (originalKey, duplicateKey) -> originalKey));

        return pushAggregationIntoTableScan.apply(node, captures.get(TABLE_SCAN), context, groupBySymbols, groupingSetSymbolMapping);
    }
}
