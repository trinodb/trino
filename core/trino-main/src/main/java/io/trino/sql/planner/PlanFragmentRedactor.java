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
import io.trino.execution.TableInfo;
import io.trino.metadata.IndexHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorIndexHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.SecureExpression;
import io.trino.sql.ir.SecureExpressions;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.ir.SecureExpression.REDACTED;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static java.util.function.Function.identity;

/**
 * Creates reporting fragments only. Executable fragments and the credentials sent to workers are never rewritten.
 */
public final class PlanFragmentRedactor
{
    private PlanFragmentRedactor() {}

    public static PlanFragment redact(PlanFragment fragment, Map<PlanNodeId, TableInfo> tables)
    {
        if (ExpressionExtractor.extractExpressions(fragment.getRoot()).stream().noneMatch(SecureExpressions::isPresent)) {
            return fragment;
        }
        PlanNode root = rewriteWith(new Visitor(SecureColumns.symbols(fragment.getRoot()), tables), fragment.getRoot());
        return new PlanFragment(
                fragment.getId(),
                root,
                fragment.getSymbols(),
                fragment.getPartitioning(),
                fragment.getPartitionCount(),
                fragment.getPartitionedSources(),
                fragment.getOutputPartitioningScheme(),
                fragment.getOutputSkewedBucketCount(),
                SecureColumns.withoutValueRanges(fragment.getStatsAndCosts()),
                fragment.getActiveCatalogs(),
                ImmutableMap.of(),
                fragment.getJsonRepresentation());
    }

    private static Expression redact(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteSecureExpression(SecureExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new SecureExpression(new Constant(node.type(), null));
            }

            @Override
            public Expression rewriteLogical(Logical node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                // Secure terms are reported as one marker: their number depends on how the policy relates to the query
                ImmutableList.Builder<Expression> terms = ImmutableList.builder();
                boolean secure = false;
                for (Expression term : node.terms()) {
                    Expression rewritten = treeRewriter.rewrite(term, context);
                    if (rewritten instanceof SecureExpression) {
                        if (secure) {
                            continue;
                        }
                        secure = true;
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
        private final Set<Symbol> secureSymbols;
        private final Map<PlanNodeId, TableInfo> tables;
        private final SymbolMapper mapper = new SymbolMapper(identity())
        {
            @Override
            public Expression map(Expression expression)
            {
                return redact(expression);
            }
        };

        private Visitor(Set<Symbol> secureSymbols, Map<PlanNodeId, TableInfo> tables)
        {
            this.secureSymbols = secureSymbols;
            this.tables = tables;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            return new FilterNode(node.getId(), context.rewrite(node.getSource()), redact(node.getPredicate()));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            return new ProjectNode(node.getId(), context.rewrite(node.getSource()), node.getAssignments().rewrite(PlanFragmentRedactor::redact));
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
                    node.getFilter().map(PlanFragmentRedactor::redact),
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
                    redact(node.getFilter()),
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
                    node.getRows().map(rows -> rows.stream().map(PlanFragmentRedactor::redact).toList()));
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            return mapper.map(node, context.rewrite(node.getSource()));
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            return mapper.map(node, context.rewrite(node.getSource()));
        }

        @Override
        public PlanNode visitPatternRecognition(PatternRecognitionNode node, RewriteContext<Void> context)
        {
            return mapper.map(node, context.rewrite(node.getSource()));
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            if (Collections.disjoint(node.getAssignments().keySet(), secureSymbols)) {
                return node;
            }
            // Connector handles are opaque and can contain derived domains and pruned partition values.
            return new TableScanNode(
                    node.getId(),
                    node.getTable().withConnectorHandle(tableHandle(node.getId())),
                    node.getOutputSymbols(),
                    redactAssignments(node.getAssignments()),
                    SecureColumns.redact(node.getEnforcedConstraint(), node, secureSymbols),
                    Optional.empty(),
                    node.isUpdateTarget(),
                    node.getUseConnectorNodePartitioning());
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, RewriteContext<Void> context)
        {
            if (Collections.disjoint(node.getAssignments().keySet(), secureSymbols)) {
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

        private RedactedTableHandle tableHandle(PlanNodeId id)
        {
            return new RedactedTableHandle(Optional.ofNullable(tables.get(id)).map(table -> table.tableName().toString()).orElse(REDACTED));
        }

        private Map<Symbol, ColumnHandle> redactAssignments(Map<Symbol, ColumnHandle> assignments)
        {
            ImmutableMap.Builder<Symbol, ColumnHandle> result = ImmutableMap.builder();
            assignments.forEach((symbol, column) -> result.put(symbol, secureSymbols.contains(symbol) ? new RedactedColumnHandle() : column));
            return result.buildOrThrow();
        }
    }

    public record RedactedTableHandle(String table)
            implements ConnectorTableHandle {}

    public record RedactedColumnHandle()
            implements ColumnHandle {}

    public record RedactedIndexHandle()
            implements ConnectorIndexHandle {}
}
