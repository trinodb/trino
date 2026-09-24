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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsAndCosts;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.execution.TableInfo;
import io.trino.metadata.IndexHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorIndexHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.SensitiveExpression;
import io.trino.sql.ir.SensitiveExpressions;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.InsertTarget;
import io.trino.sql.planner.plan.TableWriterNode.MergeTarget;
import io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.ValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.isSensitiveExpressionRedactionEnabled;
import static io.trino.sql.ir.IrUtils.preOrder;
import static io.trino.sql.ir.SensitiveExpression.REDACTED;
import static io.trino.sql.planner.SensitiveSymbols.nodeExpressions;
import static io.trino.sql.planner.SensitiveSymbols.sources;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static java.util.function.Function.identity;

/**
 * Creates reporting fragments only. The fragments sent to workers are never rewritten.
 */
public final class PlanFragmentRedactor
{
    private PlanFragmentRedactor() {}

    /**
     * Returns whether the reporting fragments of the plan must be redacted: redaction is enabled, and a fragment has a
     * sensitive expression or language functions. Every fragment is then redacted, since the statistics of a fragment
     * can derive from a sensitive expression in another one, and a sensitive expression the planner removed, like the
     * mask of a column the query does not read, still resolved its language functions.
     */
    public static boolean needsRedaction(Session session, SubPlan plan)
    {
        if (!isSensitiveExpressionRedactionEnabled(session)) {
            return false;
        }
        return plan.getAllFragments().stream()
                .anyMatch(fragment -> !fragment.getLanguageFunctions().isEmpty() || hasSensitiveExpression(fragment.getRoot()));
    }

    public static RedactedPlanFragment redact(PlanFragment fragment, Map<PlanNodeId, TableInfo> tables, boolean needsRedaction)
    {
        if (!needsRedaction) {
            return new RedactedPlanFragment(fragment);
        }
        PlanNode root = rewriteWith(new Visitor(SensitiveSymbols.inPlan(fragment.getRoot()), tables), fragment.getRoot());
        verifyRedacted(root);
        return new RedactedPlanFragment(new PlanFragment(
                fragment.getId(),
                root,
                fragment.getSymbols(),
                fragment.getPartitioning(),
                fragment.getPartitionCount(),
                fragment.getPartitionedSources(),
                fragment.getOutputPartitioningScheme(),
                fragment.getOutputSkewedBucketCount(),
                withoutValueRanges(fragment.getStatsAndCosts()),
                fragment.getActiveCatalogs(),
                ImmutableMap.of(),
                fragment.getJsonRepresentation()));
    }

    /**
     * Removes the domains of the scan columns that {@code sensitiveSymbols} constrain.
     */
    public static TupleDomain<ColumnHandle> redactConstraint(TupleDomain<ColumnHandle> constraint, TableScanNode scan, Set<Symbol> sensitiveSymbols)
    {
        Set<ColumnHandle> sensitiveColumns = scan.getAssignments().entrySet().stream()
                .filter(entry -> sensitiveSymbols.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(toImmutableSet());
        if (sensitiveColumns.isEmpty()) {
            return constraint;
        }
        return constraint.filter((column, _) -> !sensitiveColumns.contains(column));
    }

    private static StatsAndCosts withoutValueRanges(StatsAndCosts statsAndCosts)
    {
        Map<PlanNodeId, PlanNodeStatsEstimate> stats = statsAndCosts.getStats().entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> withoutValueRanges(entry.getValue())));
        return new StatsAndCosts(stats, statsAndCosts.getCosts());
    }

    private static PlanNodeStatsEstimate withoutValueRanges(PlanNodeStatsEstimate estimate)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.buildFrom(estimate);
        estimate.getSymbolStatistics().forEach((symbol, statistics) -> builder.addSymbolStatistics(
                symbol,
                SymbolStatsEstimate.buildFrom(statistics)
                        .setLowValue(Double.NEGATIVE_INFINITY)
                        .setHighValue(Double.POSITIVE_INFINITY)
                        .build()));
        return builder.build();
    }

    private static boolean hasSensitiveExpression(PlanNode node)
    {
        return expressions(node).stream().anyMatch(SensitiveExpressions::isPresent) ||
                sources(node).stream().anyMatch(PlanFragmentRedactor::hasSensitiveExpression);
    }

    // The JSON form of a sensitive expression includes the wrapped expression, so a node the visitor misses must fail
    // the query rather than leak the policy
    private static void verifyRedacted(PlanNode node)
    {
        for (Expression expression : expressions(node)) {
            boolean redacted = preOrder(expression)
                    .filter(SensitiveExpression.class::isInstance)
                    .map(SensitiveExpression.class::cast)
                    .allMatch(PlanFragmentRedactor::isPlaceholder);
            verify(redacted, "Sensitive expression is not redacted in %s", node.getClass().getSimpleName());
        }
        sources(node).forEach(PlanFragmentRedactor::verifyRedacted);
    }

    // The expressions of a node type not known here cannot be checked
    private static List<Expression> expressions(PlanNode node)
    {
        return nodeExpressions(node).orElseThrow(() -> new IllegalStateException("Cannot verify redaction of " + node.getClass().getSimpleName()));
    }

    private static boolean isPlaceholder(SensitiveExpression expression)
    {
        return expression.expression() instanceof Constant constant && constant.value() == null;
    }

    private static Expression redact(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteSensitiveExpression(SensitiveExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new SensitiveExpression(new Constant(node.type(), null));
            }

            @Override
            public Expression rewriteLogical(Logical node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                // Sensitive terms are reported as one marker: their number depends on how the policy relates to the query
                ImmutableList.Builder<Expression> terms = ImmutableList.builder();
                boolean sensitive = false;
                for (Expression term : node.terms()) {
                    Expression rewritten = treeRewriter.rewrite(term, context);
                    if (rewritten instanceof SensitiveExpression) {
                        if (sensitive) {
                            continue;
                        }
                        sensitive = true;
                    }
                    terms.add(rewritten);
                }
                List<Expression> result = terms.build();
                if (result.size() == 1) {
                    return result.getFirst();
                }
                return new Logical(node.operator(), result);
            }
        }, expression);
    }

    private static final class Visitor
            extends SimplePlanRewriter<Void>
    {
        private final Set<Symbol> sensitiveSymbols;
        private final Map<PlanNodeId, TableInfo> tables;
        private final SymbolMapper mapper = new SymbolMapper(identity())
        {
            @Override
            public Expression map(Expression expression)
            {
                return redactSensitive(expression);
            }
        };

        private Visitor(Set<Symbol> sensitiveSymbols, Map<PlanNodeId, TableInfo> tables)
        {
            this.sensitiveSymbols = sensitiveSymbols;
            this.tables = tables;
        }

        @Override
        public PlanNode visitAdaptivePlanNode(AdaptivePlanNode node, RewriteContext<Void> context)
        {
            // The initial plan is not a source, but it is reported
            return new AdaptivePlanNode(node.getId(), context.rewrite(node.getInitialPlan()), node.getInitialSymbols(), context.rewrite(node.getCurrentPlan()));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            return new FilterNode(node.getId(), context.rewrite(node.getSource()), redactSensitive(node.getPredicate()));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            return new ProjectNode(node.getId(), context.rewrite(node.getSource()), node.getAssignments().rewrite(this::redactSensitive));
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            return new JoinNode(
                    node.getId(),
                    node.getType(),
                    context.rewrite(node.getLeft()),
                    context.rewrite(node.getRight()),
                    node.getCriteria(),
                    node.getLeftOutputSymbols(),
                    node.getRightOutputSymbols(),
                    node.isMaySkipOutputDuplicates(),
                    node.getFilter().map(this::redactSensitive),
                    node.getDistributionType(),
                    node.isSpillable(),
                    node.getDynamicFilters(),
                    node.getReorderJoinStatsAndCost());
        }

        @Override
        public PlanNode visitSpatialJoin(SpatialJoinNode node, RewriteContext<Void> context)
        {
            return new SpatialJoinNode(
                    node.getId(),
                    node.getType(),
                    context.rewrite(node.getLeft()),
                    context.rewrite(node.getRight()),
                    node.getOutputSymbols(),
                    redactSensitive(node.getFilter()),
                    node.getLeftPartitionSymbol(),
                    node.getRightPartitionSymbol(),
                    node.getKdbTree());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Void> context)
        {
            return new ValuesNode(
                    node.getId(),
                    node.getOutputSymbols(),
                    node.getRowCount(),
                    node.getRows().map(rows -> rows.stream().map(this::redactSensitive).toList()));
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            return AggregationNode.builderFrom(node)
                    .setSource(context.rewrite(node.getSource()))
                    .setAggregations(node.getAggregations().entrySet().stream()
                            .collect(toImmutableMap(Map.Entry::getKey, entry -> mapper.map(entry.getValue()))))
                    .build();
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            return mapper.map(node, context.rewrite(node.getSource()));
        }

        @Override
        public PlanNode visitPatternRecognition(PatternRecognitionNode node, RewriteContext<Void> context)
        {
            // SymbolMapper does not map measure and variable definition expressions
            PatternRecognitionNode mapped = mapper.map(node, context.rewrite(node.getSource()));
            ImmutableMap.Builder<Symbol, Measure> measures = ImmutableMap.builder();
            mapped.getMeasures().forEach((symbol, measure) -> measures.put(symbol, new Measure(redactSensitive(measure.getExpressionAndValuePointers()), measure.getType())));
            ImmutableMap.Builder<IrLabel, ExpressionAndValuePointers> variableDefinitions = ImmutableMap.builder();
            mapped.getVariableDefinitions().forEach((label, definition) -> variableDefinitions.put(label, redactSensitive(definition)));
            return new PatternRecognitionNode(
                    mapped.getId(),
                    mapped.getSource(),
                    mapped.getSpecification(),
                    mapped.getPrePartitionedInputs(),
                    mapped.getPreSortedOrderPrefix(),
                    mapped.getWindowFunctions(),
                    measures.buildOrThrow(),
                    mapped.getCommonBaseFrame(),
                    mapped.getRowsPerMatch(),
                    mapped.getSkipToLabels(),
                    mapped.getSkipToPosition(),
                    mapped.isInitial(),
                    mapped.getPattern(),
                    variableDefinitions.buildOrThrow());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            if (Collections.disjoint(node.getAssignments().keySet(), sensitiveSymbols)) {
                return node;
            }
            // Connector handles are opaque and can contain derived domains and pruned partition values.
            return new TableScanNode(
                    node.getId(),
                    node.getTable().withConnectorHandle(tableHandle(node.getId())),
                    node.getOutputSymbols(),
                    redactAssignments(node.getAssignments()),
                    redactConstraint(node.getEnforcedConstraint(), node, sensitiveSymbols),
                    Optional.empty(),
                    node.isUpdateTarget(),
                    node.getUseConnectorNodePartitioning());
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, RewriteContext<Void> context)
        {
            if (Collections.disjoint(node.getAssignments().keySet(), sensitiveSymbols)) {
                return node;
            }
            IndexHandle index = node.getIndexHandle();
            return new IndexSourceNode(
                    node.getId(),
                    new IndexHandle(index.catalogHandle(), index.transactionHandle(), new RedactedIndexHandle()),
                    node.getTableHandle().withConnectorHandle(tableHandle(node.getId())),
                    node.getLookupSymbols(),
                    node.getOutputSymbols(),
                    redactAssignments(node.getAssignments()));
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Void> context)
        {
            return new TableWriterNode(
                    node.getId(),
                    context.rewrite(node.getSource()),
                    redactTarget(node.getTarget()),
                    node.getRowCountSymbol(),
                    node.getFragmentSymbol(),
                    node.getColumns(),
                    node.getColumnNames(),
                    node.getPartitioningScheme(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<Void> context)
        {
            return new TableFinishNode(
                    node.getId(),
                    context.rewrite(node.getSource()),
                    redactTarget(node.getTarget()),
                    node.getOutputSymbols(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        @Override
        public PlanNode visitMergeWriter(MergeWriterNode node, RewriteContext<Void> context)
        {
            return new MergeWriterNode(
                    node.getId(),
                    context.rewrite(node.getSource()),
                    redactMergeTarget(node.getTarget()),
                    node.getProjectedSymbols(),
                    node.getPartitioningScheme(),
                    node.getOutputSymbols());
        }

        private Expression redactSensitive(Expression expression)
        {
            if (!SensitiveExpressions.isPresent(expression)) {
                return expression;
            }
            return redact(expression);
        }

        private ExpressionAndValuePointers redactSensitive(ExpressionAndValuePointers expression)
        {
            return new ExpressionAndValuePointers(
                    redactSensitive(expression.getExpression()),
                    expression.getAssignments().stream()
                            .map(assignment -> new ExpressionAndValuePointers.Assignment(assignment.symbol(), redactSensitive(assignment.valuePointer())))
                            .collect(toImmutableList()));
        }

        private ValuePointer redactSensitive(ValuePointer pointer)
        {
            if (!(pointer instanceof AggregationValuePointer aggregation)) {
                return pointer;
            }
            return new AggregationValuePointer(
                    aggregation.getFunction(),
                    aggregation.getSetDescriptor(),
                    aggregation.getArguments().stream()
                            .map(this::redactSensitive)
                            .collect(toImmutableList()),
                    aggregation.getClassifierSymbol(),
                    aggregation.getMatchNumberSymbol());
        }

        private RedactedTableHandle tableHandle(PlanNodeId id)
        {
            return new RedactedTableHandle(Optional.ofNullable(tables.get(id)).map(table -> table.tableName().toString()).orElse(REDACTED));
        }

        // Writers keep the handles of the scans they read, and those can hold domains derived from a sensitive
        // expression in another fragment
        private static WriterTarget redactTarget(WriterTarget target)
        {
            return switch (target) {
                case InsertTarget insert -> new InsertTarget(
                        insert.getHandle(),
                        insert.getSchemaTableName(),
                        insert.isMultipleWritersPerPartitionSupported(),
                        // Used only by the planner, and not serialized
                        OptionalInt.empty(),
                        insert.getWriterScalingOptions(),
                        ImmutableList.of());
                case MergeTarget merge -> redactMergeTarget(merge);
                default -> target;
            };
        }

        private static MergeTarget redactMergeTarget(MergeTarget target)
        {
            return new MergeTarget(
                    target.getHandle().withConnectorHandle(new RedactedTableHandle(target.getSchemaTableName().toString())),
                    Optional.empty(),
                    target.getSchemaTableName(),
                    target.getMergeParadigmAndTypes(),
                    ImmutableList.of(),
                    target.getUpdateCaseColumnHandles());
        }

        private Map<Symbol, ColumnHandle> redactAssignments(Map<Symbol, ColumnHandle> assignments)
        {
            ImmutableMap.Builder<Symbol, ColumnHandle> result = ImmutableMap.builder();
            assignments.forEach((symbol, column) -> result.put(symbol, sensitiveSymbols.contains(symbol) ? new RedactedColumnHandle() : column));
            return result.buildOrThrow();
        }
    }

    public record RedactedTableHandle(String table)
            implements ConnectorTableHandle
    {
        @Override
        public String toString()
        {
            return table;
        }
    }

    public record RedactedColumnHandle()
            implements ColumnHandle {}

    public record RedactedIndexHandle()
            implements ConnectorIndexHandle {}
}
