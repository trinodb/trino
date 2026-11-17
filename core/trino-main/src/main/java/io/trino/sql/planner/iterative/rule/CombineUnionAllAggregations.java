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
import com.google.common.collect.ImmutableMap;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Array;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.globalAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.Patterns.union;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites a {@code UNION ALL} of global aggregations over one table:
 * <pre>
 * - Union
 *     - Project (constants)
 *         - Aggregation (count, sum, ...)
 *             - Filter (branch predicate)
 *                 - TableScan
 *     - Project (constants)
 *         - Aggregation
 *             - Filter (another predicate)
 *                 - TableScan (same table)
 * </pre>
 * into one scan. Each branch becomes {@code FILTER (WHERE ...)} on that scan,
 * and {@code UNNEST} of an array of rows restores one output row per branch.
 * <p>
 * The rule runs with {@code MergeUnion}, before predicate pushdown and
 * {@code AddExchanges}. {@code ImplementFilteredAggregations} later turns the
 * filters into masks.
 */
public class CombineUnionAllAggregations
        implements Rule<UnionNode>
{
    private static final Pattern<UnionNode> PATTERN = union()
            .matching(unionNode -> unionNode.getSources().size() >= 2);

    @Override
    public Pattern<UnionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(UnionNode node, Captures captures, Context context)
    {
        return combine(node, context)
                .map(Result::ofPlanNode)
                .orElseGet(Result::empty);
    }

    private Optional<PlanNode> combine(UnionNode union, Context context)
    {
        Lookup lookup = context.getLookup();
        ImmutableList.Builder<Candidate> candidates = ImmutableList.builder();
        for (PlanNode source : union.getSources()) {
            Optional<Candidate> candidate = parseCandidate(source, lookup);
            if (candidate.isEmpty()) {
                return Optional.empty();
            }
            candidates.add(candidate.get());
        }
        return buildCombined(union, candidates.build(), context);
    }

    private Optional<PlanNode> buildCombined(UnionNode union, List<Candidate> candidates, Context context)
    {
        Optional<TableScanNode> canonicalScan = canonicalScan(candidates);
        if (canonicalScan.isEmpty()) {
            return Optional.empty();
        }
        TableScanNode scan = canonicalScan.get();
        Map<ColumnHandle, Symbol> canonicalColumns = columnsByHandle(scan).orElseThrow();

        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        Assignments.Builder inputs = Assignments.builder()
                .putIdentities(scan.getOutputSymbols());
        ImmutableList.Builder<Expression> rows = ImmutableList.builder();
        List<Symbol> unionOutputs = union.getOutputSymbols();
        RowType rowType = RowType.anonymous(unionOutputs.stream()
                .map(Symbol::type)
                .collect(toImmutableList()));

        for (int branchIndex = 0; branchIndex < candidates.size(); branchIndex++) {
            Candidate candidate = candidates.get(branchIndex);
            Optional<Map<Symbol, Expression>> symbolMapping = symbolMapping(candidate.scan(), canonicalColumns);
            if (symbolMapping.isEmpty()) {
                return Optional.empty();
            }

            Expression predicate = rewriteReferences(candidate.predicate(), symbolMapping.get());
            if (!scan.getOutputSymbols().containsAll(SymbolsExtractor.extractUnique(predicate))) {
                return Optional.empty();
            }

            Optional<Symbol> filter = Optional.empty();
            if (!predicate.equals(TRUE)) {
                if (!predicate.type().equals(BOOLEAN)) {
                    return Optional.empty();
                }
                Symbol filterSymbol = context.getSymbolAllocator().newSymbol("branch_filter", BOOLEAN);
                inputs.put(filterSymbol, predicate);
                filter = Optional.of(filterSymbol);
            }

            Map<Symbol, Expression> aggregatedOutputs = new HashMap<>();
            for (AggregationSpec specification : candidate.aggregations()) {
                ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
                for (Expression argument : specification.arguments()) {
                    Expression rewritten = rewriteReferences(argument, symbolMapping.get());
                    if (!(rewritten instanceof Reference reference)) {
                        return Optional.empty();
                    }
                    Symbol symbol = Symbol.from(reference);
                    if (!scan.getOutputSymbols().contains(symbol)) {
                        return Optional.empty();
                    }
                    arguments.add(rewritten);
                }
                Symbol output = context.getSymbolAllocator().newSymbol(specification.symbol());
                aggregations.put(output, new Aggregation(
                        specification.function(),
                        arguments.build(),
                        false,
                        filter,
                        Optional.empty(),
                        Optional.empty()));
                aggregatedOutputs.put(specification.symbol(), output.toSymbolReference());
            }

            List<Symbol> layout = union.sourceOutputLayout(branchIndex);
            Set<Symbol> produced = aggregatedOutputs.values().stream()
                    .map(Symbol::from)
                    .collect(toImmutableSet());
            ImmutableList.Builder<Expression> fields = ImmutableList.builder();
            for (int field = 0; field < layout.size(); field++) {
                Expression expression = candidate.outputs().get(layout.get(field));
                if (expression == null) {
                    return Optional.empty();
                }
                Expression rewritten = rewriteReferences(expression, aggregatedOutputs);
                if (!rewritten.type().equals(unionOutputs.get(field).type())
                        || !produced.containsAll(SymbolsExtractor.extractUnique(rewritten))) {
                    return Optional.empty();
                }
                fields.add(rewritten);
            }
            rows.add(new Row(fields.build(), rowType));
        }

        PlanNode source = new TableScanNode(
                context.getIdAllocator().getNextId(),
                scan.getTable(),
                scan.getOutputSymbols(),
                scan.getAssignments(),
                scan.getEnforcedConstraint(),
                scan.getStatistics(),
                scan.isUpdateTarget(),
                scan.getUseConnectorNodePartitioning());
        PlanNode aggregationSource = new ProjectNode(
                context.getIdAllocator().getNextId(),
                source,
                inputs.build());
        AggregationNode aggregation = singleAggregation(
                context.getIdAllocator().getNextId(),
                aggregationSource,
                aggregations.buildOrThrow(),
                globalAggregation());

        Symbol rowArray = context.getSymbolAllocator().newSymbol("union_rows", new ArrayType(rowType));
        ProjectNode array = new ProjectNode(
                context.getIdAllocator().getNextId(),
                aggregation,
                Assignments.of(rowArray, new Array(rowType, rows.build())));

        ImmutableList.Builder<Symbol> unnestedFields = ImmutableList.builder();
        Assignments.Builder outputs = Assignments.builder();
        for (Symbol output : unionOutputs) {
            Symbol field = context.getSymbolAllocator().newSymbol(output);
            unnestedFields.add(field);
            outputs.put(output, field.toSymbolReference());
        }
        UnnestNode unnest = new UnnestNode(
                context.getIdAllocator().getNextId(),
                array,
                ImmutableList.of(),
                ImmutableList.of(new UnnestNode.Mapping(rowArray, unnestedFields.build())),
                Optional.empty(),
                INNER);
        return Optional.of(new ProjectNode(
                context.getIdAllocator().getNextId(),
                unnest,
                outputs.build()));
    }

    private static Optional<TableScanNode> canonicalScan(List<Candidate> candidates)
    {
        for (Candidate candidate : candidates) {
            TableScanNode scan = candidate.scan();
            if (columnsByHandle(scan).isEmpty()) {
                return Optional.empty();
            }
            boolean coversAll = candidates.stream().allMatch(other -> compatible(scan, other.scan()));
            if (coversAll) {
                return Optional.of(scan);
            }
        }
        return Optional.empty();
    }

    private static boolean compatible(TableScanNode canonical, TableScanNode other)
    {
        Optional<Map<ColumnHandle, Symbol>> canonicalColumns = columnsByHandle(canonical);
        Optional<Map<ColumnHandle, Symbol>> otherColumns = columnsByHandle(other);
        if (canonicalColumns.isEmpty() || otherColumns.isEmpty()) {
            return false;
        }
        return canonical.getTable().equals(other.getTable())
                && canonical.getEnforcedConstraint().equals(other.getEnforcedConstraint())
                && canonical.isUpdateTarget() == other.isUpdateTarget()
                && canonicalColumns.get().keySet().containsAll(otherColumns.get().keySet());
    }

    /**
     * Maps a branch scan symbol to the canonical scan symbol for the same column.
     */
    private static Optional<Map<Symbol, Expression>> symbolMapping(TableScanNode scan, Map<ColumnHandle, Symbol> canonicalColumns)
    {
        Map<Symbol, Expression> mapping = new HashMap<>();
        for (Map.Entry<Symbol, ColumnHandle> entry : scan.getAssignments().entrySet()) {
            Symbol canonical = canonicalColumns.get(entry.getValue());
            if (canonical == null) {
                return Optional.empty();
            }
            mapping.put(entry.getKey(), canonical.toSymbolReference());
        }
        return Optional.of(mapping);
    }

    private static Optional<Map<ColumnHandle, Symbol>> columnsByHandle(TableScanNode scan)
    {
        Map<ColumnHandle, Symbol> columns = new HashMap<>();
        for (Map.Entry<Symbol, ColumnHandle> entry : scan.getAssignments().entrySet()) {
            if (columns.put(entry.getValue(), entry.getKey()) != null) {
                return Optional.empty();
            }
        }
        return Optional.of(columns);
    }

    private static Optional<Candidate> parseCandidate(PlanNode source, Lookup lookup)
    {
        PlanNode node = lookup.resolve(source);
        Map<Symbol, Expression> outputs = identityAssignments(node);
        while (node instanceof ProjectNode project) {
            if (project.getAssignments().expressions().stream().anyMatch(expression -> !isDeterministic(expression))) {
                return Optional.empty();
            }
            outputs = inlineAssignments(outputs, project.getAssignments());
            node = lookup.resolve(project.getSource());
        }
        if (!(node instanceof AggregationNode aggregation) || !isSimpleGlobalAggregation(aggregation)) {
            return Optional.empty();
        }

        Set<Symbol> aggregationOutputs = Set.copyOf(aggregation.getOutputSymbols());
        for (Expression expression : outputs.values()) {
            if (!isDeterministic(expression) || !aggregationOutputs.containsAll(SymbolsExtractor.extractUnique(expression))) {
                return Optional.empty();
            }
        }

        Optional<ScanInput> input = extractInput(aggregation.getSource(), lookup);
        if (input.isEmpty()) {
            return Optional.empty();
        }

        ImmutableList.Builder<AggregationSpec> aggregations = ImmutableList.builder();
        for (Map.Entry<Symbol, Aggregation> entry : aggregation.getAggregations().entrySet()) {
            Aggregation value = entry.getValue();
            ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
            for (Expression argument : value.getArguments()) {
                if (!(argument instanceof Reference)) {
                    return Optional.empty();
                }
                Expression rewritten = rewriteReferences(argument, input.get().symbols());
                if (!(rewritten instanceof Reference)) {
                    return Optional.empty();
                }
                arguments.add(rewritten);
            }
            aggregations.add(new AggregationSpec(entry.getKey(), value.getResolvedFunction(), arguments.build()));
        }

        return Optional.of(new Candidate(
                input.get().scan(),
                input.get().predicate(),
                aggregations.build(),
                outputs));
    }

    private static boolean isSimpleGlobalAggregation(AggregationNode aggregation)
    {
        if (!aggregation.hasSingleGlobalAggregation()
                || aggregation.getStep() != SINGLE
                || aggregation.getGroupIdSymbol().isPresent()
                || aggregation.getAggregations().isEmpty()
                || aggregation.hasOrderings()) {
            return false;
        }
        return aggregation.getAggregations().values().stream().allMatch(aggregationCall ->
                !aggregationCall.isDistinct()
                        && aggregationCall.getFilter().isEmpty()
                        && aggregationCall.getMask().isEmpty()
                        && aggregationCall.getOrderingScheme().isEmpty());
    }

    private static Optional<ScanInput> extractInput(PlanNode node, Lookup lookup)
    {
        node = lookup.resolve(node);
        if (node instanceof TableScanNode scan) {
            if (scan.isUpdateTarget()) {
                return Optional.empty();
            }
            return Optional.of(new ScanInput(scan, TRUE, identityAssignments(scan)));
        }
        if (node instanceof FilterNode filter) {
            if (!isSimplePredicate(filter.getPredicate())) {
                return Optional.empty();
            }
            Optional<ScanInput> input = extractInput(filter.getSource(), lookup);
            if (input.isEmpty()) {
                return Optional.empty();
            }
            Expression predicate = rewriteReferences(filter.getPredicate(), input.get().symbols());
            if (!isSimplePredicate(predicate)) {
                return Optional.empty();
            }
            Expression combined = input.get().predicate().equals(TRUE)
                    ? predicate
                    : and(predicate, input.get().predicate());
            return Optional.of(new ScanInput(input.get().scan(), combined, input.get().symbols()));
        }
        if (node instanceof ProjectNode project) {
            if (project.getAssignments().expressions().stream().anyMatch(expression -> !isDeterministic(expression))) {
                return Optional.empty();
            }
            Optional<ScanInput> input = extractInput(project.getSource(), lookup);
            if (input.isEmpty()) {
                return Optional.empty();
            }
            ImmutableMap.Builder<Symbol, Expression> symbols = ImmutableMap.builder();
            for (Map.Entry<Symbol, Expression> assignment : project.getAssignments().entrySet()) {
                Expression rewritten = rewriteReferences(assignment.getValue(), input.get().symbols());
                if (!isDeterministic(rewritten)) {
                    return Optional.empty();
                }
                symbols.put(assignment.getKey(), rewritten);
            }
            return Optional.of(new ScanInput(input.get().scan(), input.get().predicate(), symbols.buildOrThrow()));
        }
        return Optional.empty();
    }

    private static boolean isSimplePredicate(Expression predicate)
    {
        return isDeterministic(predicate) && extractDynamicFilters(predicate).dynamicConjuncts().isEmpty();
    }

    private static Map<Symbol, Expression> identityAssignments(PlanNode node)
    {
        ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap.builder();
        for (Symbol symbol : node.getOutputSymbols()) {
            assignments.put(symbol, symbol.toSymbolReference());
        }
        return assignments.buildOrThrow();
    }

    private static Map<Symbol, Expression> inlineAssignments(Map<Symbol, Expression> outputs, Assignments assignments)
    {
        Map<Symbol, Expression> mapping = new HashMap<>();
        for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
            mapping.put(assignment.getKey(), assignment.getValue());
        }
        ImmutableMap.Builder<Symbol, Expression> inlined = ImmutableMap.builder();
        for (Map.Entry<Symbol, Expression> output : outputs.entrySet()) {
            inlined.put(output.getKey(), rewriteReferences(output.getValue(), mapping));
        }
        return inlined.buildOrThrow();
    }

    private static Expression rewriteReferences(Expression expression, Map<Symbol, Expression> mapping)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteReference(Reference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression replacement = mapping.get(Symbol.from(node));
                if (replacement == null) {
                    return node;
                }
                return replacement;
            }
        }, expression);
    }

    private record Candidate(TableScanNode scan, Expression predicate, List<AggregationSpec> aggregations, Map<Symbol, Expression> outputs)
    {
        private Candidate
        {
            requireNonNull(scan, "scan is null");
            requireNonNull(predicate, "predicate is null");
            aggregations = ImmutableList.copyOf(requireNonNull(aggregations, "aggregations is null"));
            outputs = ImmutableMap.copyOf(requireNonNull(outputs, "outputs is null"));
        }
    }

    private record AggregationSpec(Symbol symbol, ResolvedFunction function, List<Expression> arguments)
    {
        private AggregationSpec
        {
            requireNonNull(symbol, "symbol is null");
            requireNonNull(function, "function is null");
            arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
        }
    }

    private record ScanInput(TableScanNode scan, Expression predicate, Map<Symbol, Expression> symbols)
    {
        private ScanInput
        {
            requireNonNull(scan, "scan is null");
            requireNonNull(predicate, "predicate is null");
            symbols = ImmutableMap.copyOf(requireNonNull(symbols, "symbols is null"));
        }
    }
}
