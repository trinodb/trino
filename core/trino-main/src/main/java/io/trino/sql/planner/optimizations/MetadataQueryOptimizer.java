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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.TableProperties;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Row;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static java.util.Objects.requireNonNull;

/**
 * Converts cardinality-insensitive aggregations (max, min, "distinct") over partition keys
 * into simple metadata queries
 */
public class MetadataQueryOptimizer
        implements PlanOptimizer
{
    private static final Set<String> ALLOWED_FUNCTIONS = ImmutableSet.of("max", "min", "approx_distinct");

    private final PlannerContext plannerContext;

    public MetadataQueryOptimizer(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector, TableStatsProvider tableStatsProvider)
    {
        if (!SystemSessionProperties.isOptimizeMetadataQueries(session)) {
            return plan;
        }
        return SimplePlanRewriter.rewriteWith(new Optimizer(session, plannerContext, idAllocator), plan, null);
    }

    private static class Optimizer
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;
        private final PlannerContext plannerContext;
        private final LiteralEncoder literalEncoder;

        private Optimizer(Session session, PlannerContext plannerContext, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.plannerContext = plannerContext;
            this.literalEncoder = new LiteralEncoder(plannerContext);
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            // supported functions are only MIN/MAX/APPROX_DISTINCT or distinct aggregates
            for (Aggregation aggregation : node.getAggregations().values()) {
                if (!ALLOWED_FUNCTIONS.contains(aggregation.getResolvedFunction().getSignature().getName()) && !aggregation.isDistinct()) {
                    return context.defaultRewrite(node);
                }
            }

            Optional<TableScanNode> result = findTableScan(node.getSource());
            if (result.isEmpty()) {
                return context.defaultRewrite(node);
            }

            // verify all outputs of table scan are partition keys
            TableScanNode tableScan = result.get();

            ImmutableMap.Builder<Symbol, Type> typesBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, ColumnHandle> columnBuilder = ImmutableMap.builder();

            List<Symbol> inputs = tableScan.getOutputSymbols();
            if (inputs.isEmpty()) {
                return context.defaultRewrite(node);
            }
            for (Symbol symbol : inputs) {
                ColumnHandle column = tableScan.getAssignments().get(symbol);
                ColumnMetadata columnMetadata = plannerContext.getMetadata().getColumnMetadata(session, tableScan.getTable(), column);

                typesBuilder.put(symbol, columnMetadata.getType());
                columnBuilder.put(symbol, column);
            }

            Map<Symbol, ColumnHandle> columns = columnBuilder.buildOrThrow();
            Map<Symbol, Type> types = typesBuilder.buildOrThrow();

            // Materialize the list of partitions and replace the TableScan node
            // with a Values node
            TableProperties layout = plannerContext.getMetadata().getTableProperties(session, tableScan.getTable());
            if (layout.getDiscretePredicates().isEmpty()) {
                return context.defaultRewrite(node);
            }
            DiscretePredicates predicates = layout.getDiscretePredicates().get();

            // the optimization is only valid if the aggregation node only relies on partition keys
            if (!predicates.getColumns().containsAll(columns.values())) {
                return context.defaultRewrite(node);
            }

            ImmutableList.Builder<Expression> rowsBuilder = ImmutableList.builder();
            for (TupleDomain<ColumnHandle> domain : predicates.getPredicates()) {
                if (!domain.isNone()) {
                    Map<ColumnHandle, NullableValue> entries = TupleDomain.extractFixedValues(domain).get();

                    ImmutableList.Builder<Expression> rowBuilder = ImmutableList.builder();
                    // for each input column, add a literal expression using the entry value
                    for (Symbol input : inputs) {
                        ColumnHandle column = columns.get(input);
                        Type type = types.get(input);
                        NullableValue value = entries.get(column);
                        if (value == null) {
                            // partition key does not have a single value, so bail out to be safe
                            return context.defaultRewrite(node);
                        }
                        rowBuilder.add(literalEncoder.toExpression(session, value.getValue(), type));
                    }
                    rowsBuilder.add(new Row(rowBuilder.build()));
                }
            }

            // replace the tablescan node with a values node
            ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), inputs, rowsBuilder.build());
            return SimplePlanRewriter.rewriteWith(new Replacer(valuesNode), node);
        }

        private Optional<TableScanNode> findTableScan(PlanNode source)
        {
            while (true) {
                // allow any chain of linear transformations
                if (source instanceof MarkDistinctNode ||
                        source instanceof FilterNode ||
                        source instanceof LimitNode ||
                        source instanceof TopNNode ||
                        source instanceof SortNode) {
                    source = source.getSources().get(0);
                }
                else if (source instanceof ProjectNode project) {
                    // verify projections are deterministic
                    if (!Iterables.all(project.getAssignments().getExpressions(), expression -> isDeterministic(expression, plannerContext.getMetadata()))) {
                        return Optional.empty();
                    }
                    source = project.getSource();
                }
                else if (source instanceof TableScanNode tableScanNode) {
                    return Optional.of(tableScanNode);
                }
                else {
                    return Optional.empty();
                }
            }
        }
    }

    private static class Replacer
            extends SimplePlanRewriter<Void>
    {
        private final ValuesNode replacement;

        private Replacer(ValuesNode replacement)
        {
            this.replacement = replacement;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            return replacement;
        }
    }
}
