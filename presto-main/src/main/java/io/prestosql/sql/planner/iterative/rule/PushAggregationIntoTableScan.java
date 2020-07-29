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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.BoundSignature;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.AggregationApplicationResult;
import io.prestosql.spi.connector.Assignment;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SortItem;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.Variable;
import io.prestosql.sql.planner.ConnectorExpressionTranslator;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.GroupIdNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.matching.Pattern.typeOf;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;

public abstract class PushAggregationIntoTableScan
        implements Rule<AggregationNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

    private final Metadata metadata;

    public PushAggregationIntoTableScan(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    private static boolean allArgumentsAreSimpleReferences(AggregationNode node)
    {
        return node.getAggregations()
                .values().stream()
                .flatMap(aggregation -> aggregation.getArguments().stream())
                .allMatch(SymbolReference.class::isInstance);
    }

    private static boolean hasNoMasks(AggregationNode node)
    {
        return !node.getAggregations()
                .values().stream()
                .map(aggregation -> aggregation.getMask().isPresent())
                .anyMatch(isMaskPresent -> isMaskPresent);
    }

    protected abstract List<List<Symbol>> groupBySymbols(AggregationNode node, Captures captures);

    protected abstract Map<Symbol, Symbol> groupingSetSymbolMapping(AggregationNode node, Captures captures);

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);
        Map<String, ColumnHandle> assignments = tableScan.getAssignments()
                .entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Entry::getValue));

        List<Entry<Symbol, AggregationNode.Aggregation>> aggregations = node.getAggregations()
                .entrySet().stream()
                .collect(toImmutableList());

        List<AggregateFunction> aggregateFunctions = aggregations.stream()
                .map(Entry::getValue)
                .map(aggregation -> toAggregateFunction(context, aggregation))
                .collect(toImmutableList());

        List<Symbol> aggregationOutputSymbols = aggregations.stream()
                .map(Entry::getKey)
                .collect(toImmutableList());

        List<List<ColumnHandle>> groupByColumns = groupBySymbols(node, captures).stream()
                .map(groupingSetColumns -> groupingSetColumns.stream()
                        .map(groupByColumn -> assignments.get(groupByColumn.getName()))
                        .collect(toImmutableList()))
                .collect(toImmutableList());

        Optional<AggregationApplicationResult<TableHandle>> aggregationPushdownResult = metadata.applyAggregation(
                context.getSession(),
                tableScan.getTable(),
                aggregateFunctions,
                assignments,
                groupByColumns);

        if (aggregationPushdownResult.isEmpty()) {
            return Result.empty();
        }

        AggregationApplicationResult<TableHandle> result = aggregationPushdownResult.get();

        // The new scan outputs should be the symbols associated with grouping columns plus the symbols associated with aggregations.
        ImmutableList.Builder<Symbol> newScanOutputs = new ImmutableList.Builder<>();
        newScanOutputs.addAll(tableScan.getOutputSymbols());

        ImmutableBiMap.Builder<Symbol, ColumnHandle> newScanAssignments = new ImmutableBiMap.Builder<>();
        newScanAssignments.putAll(tableScan.getAssignments());

        Map<String, Symbol> variableMappings = new HashMap<>();

        for (Assignment assignment : result.getAssignments()) {
            Symbol symbol = context.getSymbolAllocator().newSymbol(assignment.getVariable(), assignment.getType());

            newScanOutputs.add(symbol);
            newScanAssignments.put(symbol, assignment.getColumn());
            variableMappings.put(assignment.getVariable(), symbol);
        }

        List<Expression> newProjections = result.getProjections().stream()
                .map(expression -> ConnectorExpressionTranslator.translate(expression, variableMappings, new LiteralEncoder(metadata)))
                .collect(toImmutableList());

        verify(aggregationOutputSymbols.size() == newProjections.size());

        Assignments.Builder assignmentBuilder = Assignments.builder();
        Set<Symbol> newOutputSymbols = new HashSet<>();
        IntStream.range(0, aggregationOutputSymbols.size())
                .forEach(index -> {
                    Symbol symbol = aggregationOutputSymbols.get(index);
                    newOutputSymbols.add(symbol);
                    assignmentBuilder.put(symbol, newProjections.get(index));
                });

        ImmutableBiMap<Symbol, ColumnHandle> scanAssignments = newScanAssignments.build();
        ImmutableBiMap<ColumnHandle, Symbol> columnHandleToSymbol = scanAssignments.inverse();
        // projections assignmentBuilder should have both agg and group by so we add all the group bys as symbol references
        groupingSetSymbolMapping(node, captures).forEach((key, value) -> {
            // if the connector returned a new mapping from oldColumnHandle to newColumnHandle, groupBy needs to point to
            // new columnHandle's symbol reference, otherwise it will continue pointing at oldColumnHandle.
            ColumnHandle originalColumnHandle = assignments.get(value.getName());
            ColumnHandle groupByColumnHandle = result.getGroupingColumnMapping().getOrDefault(originalColumnHandle, originalColumnHandle);

            newOutputSymbols.add(key);
            assignmentBuilder.put(key, columnHandleToSymbol.get(groupByColumnHandle).toSymbolReference());
        });

        Set<Symbol> missingSymbols = node.getOutputSymbols().stream()
                .filter(symbol -> !newOutputSymbols.contains(symbol))
                .collect(toSet());

        // The only missing symbols should all be groupId symbols that should be pruned as they will be unused
        // but we still need to return them to keep the output symbols same as before.
        verify(missingSymbols.stream().allMatch(symbol -> symbol.getName().toLowerCase(Locale.ENGLISH).startsWith("groupid")));

        missingSymbols.forEach(symbol -> assignmentBuilder.put(symbol, symbol.toSymbolReference()));

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        TableScanNode.newInstance(
                                context.getIdAllocator().getNextId(),
                                result.getHandle(),
                                newScanOutputs.build(),
                                scanAssignments),
                        assignmentBuilder.build()));
    }

    private static AggregateFunction toAggregateFunction(Context context, AggregationNode.Aggregation aggregation)
    {
        BoundSignature signature = aggregation.getResolvedFunction().getSignature();

        ImmutableList.Builder<ConnectorExpression> arguments = new ImmutableList.Builder<>();
        for (int i = 0; i < aggregation.getArguments().size(); i++) {
            SymbolReference argument = (SymbolReference) aggregation.getArguments().get(i);
            arguments.add(new Variable(argument.getName(), signature.getArgumentTypes().get(i)));
        }

        Optional<OrderingScheme> orderingScheme = aggregation.getOrderingScheme();
        Optional<List<SortItem>> sortBy = orderingScheme.map(OrderingScheme::toSortItems);

        Optional<ConnectorExpression> filter = aggregation.getFilter()
                .map(symbol -> new Variable(symbol.getName(), context.getSymbolAllocator().getTypes().get(symbol)));

        return new AggregateFunction(
                signature.getName(),
                signature.getReturnType(),
                arguments.build(),
                sortBy.orElse(ImmutableList.of()),
                aggregation.isDistinct(),
                filter);
    }

    public static class PushAggregationIntoTableScanWithoutGroupingSets
            extends PushAggregationIntoTableScan
    {
        private static final Pattern<AggregationNode> PATTERN =
                aggregation()
                        .matching(PushAggregationIntoTableScan::allArgumentsAreSimpleReferences)
                        .matching(PushAggregationIntoTableScan::hasNoMasks)
                        .with(source().matching(tableScan().capturedAs(TABLE_SCAN)));

        public PushAggregationIntoTableScanWithoutGroupingSets(Metadata metadata)
        {
            super(metadata);
        }

        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public List<List<Symbol>> groupBySymbols(AggregationNode node, Captures captures)
        {
            return List.of(node.getGroupingSets().getGroupingKeys());
        }

        @Override
        protected Map<Symbol, Symbol> groupingSetSymbolMapping(AggregationNode node, Captures captures)
        {
            return node.getGroupingSets().getGroupingKeys().stream()
                    .collect(Collectors.toMap(identity(), identity()));
        }
    }

    public static class PushAggregationIntoTableScanWithGroupingSets
            extends PushAggregationIntoTableScan
    {
        private static final Capture<GroupIdNode> GROUP_ID = newCapture();
        private static final Pattern<AggregationNode> PATTERN =
                aggregation()
                        .matching(PushAggregationIntoTableScan::allArgumentsAreSimpleReferences)
                        .matching(PushAggregationIntoTableScan::hasNoMasks)
                        .with(source().matching(typeOf(GroupIdNode.class).capturedAs(GROUP_ID)
                                .with(source().matching(tableScan().capturedAs(TABLE_SCAN)))));

        public PushAggregationIntoTableScanWithGroupingSets(Metadata metadata)
        {
            super(metadata);
        }

        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public List<List<Symbol>> groupBySymbols(AggregationNode node, Captures captures)
        {
            GroupIdNode groupIdNode = captures.get(GROUP_ID);

            return groupIdNode.getGroupingSets().stream()
                    .map(groupIdNodeSymbols -> groupIdNodeSymbols.stream()
                            .map(groupIdNode.getGroupingColumns()::get)
                            .collect(toImmutableList()))
                    .collect(toImmutableList());
        }

        @Override
        protected Map<Symbol, Symbol> groupingSetSymbolMapping(AggregationNode node, Captures captures)
        {
            GroupIdNode groupIdNode = captures.get(GROUP_ID);
            return groupIdNode.getGroupingSets().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toMap(identity(), groupIdNode.getGroupingColumns()::get, (originalKey, duplicateKey) -> originalKey));
        }
    }
}
