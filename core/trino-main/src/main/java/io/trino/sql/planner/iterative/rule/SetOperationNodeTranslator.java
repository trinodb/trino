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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.Type;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SetOperationNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.plan.WindowNode.Specification;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.trino.sql.tree.WindowFrame.Type.ROWS;
import static java.util.Objects.requireNonNull;

public class SetOperationNodeTranslator
{
    private static final String MARKER = "marker";
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final ResolvedFunction countFunction;
    private final ResolvedFunction rowNumberFunction;

    public SetOperationNodeTranslator(Session session, Metadata metadata, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        this.symbolAllocator = requireNonNull(symbolAllocator, "SymbolAllocator is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(metadata, "metadata is null");
        this.countFunction = metadata.resolveFunction(session, QualifiedName.of("count"), fromTypes(BOOLEAN));
        this.rowNumberFunction = metadata.resolveFunction(session, QualifiedName.of("row_number"), ImmutableList.of());
    }

    public TranslationResult makeSetContainmentPlanForDistinct(SetOperationNode node)
    {
        checkArgument(!(node instanceof UnionNode), "Cannot simplify a UnionNode");
        List<Symbol> markers = allocateSymbols(node.getSources().size(), MARKER, BOOLEAN);
        // identity projection for all the fields in each of the sources plus marker columns
        List<PlanNode> withMarkers = appendMarkers(markers, node.getSources(), node);

        // add a union over all the rewritten sources. The outputs of the union have the same name as the
        // original intersect node
        List<Symbol> outputs = node.getOutputSymbols();
        UnionNode union = union(withMarkers, ImmutableList.copyOf(concat(outputs, markers)));

        // add count aggregations
        List<Symbol> aggregationOutputs = allocateSymbols(markers.size(), "count", BIGINT);
        AggregationNode aggregation = computeCounts(union, outputs, markers, aggregationOutputs);

        return new TranslationResult(aggregation, aggregationOutputs);
    }

    public TranslationResult makeSetContainmentPlanForAll(SetOperationNode node)
    {
        checkArgument(!(node instanceof UnionNode), "Cannot simplify a UnionNode");
        List<Symbol> markers = allocateSymbols(node.getSources().size(), MARKER, BOOLEAN);
        // identity projection for all the fields in each of the sources plus marker columns
        List<PlanNode> withMarkers = appendMarkers(markers, node.getSources(), node);

        // add a union over all the rewritten sources
        List<Symbol> outputs = node.getOutputSymbols();
        UnionNode union = union(withMarkers, ImmutableList.copyOf(concat(outputs, markers)));

        // add counts and row number
        List<Symbol> countOutputs = allocateSymbols(markers.size(), "count", BIGINT);
        Symbol rowNumberSymbol = symbolAllocator.newSymbol("row_number", BIGINT);
        WindowNode window = appendCounts(union, outputs, markers, countOutputs, rowNumberSymbol);

        // prune markers
        ProjectNode project = new ProjectNode(
                idAllocator.getNextId(),
                window,
                Assignments.identity(ImmutableList.copyOf(concat(outputs, countOutputs, ImmutableList.of(rowNumberSymbol)))));

        return new TranslationResult(project, countOutputs, Optional.of(rowNumberSymbol));
    }

    private List<Symbol> allocateSymbols(int count, String nameHint, Type type)
    {
        ImmutableList.Builder<Symbol> symbolsBuilder = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            symbolsBuilder.add(symbolAllocator.newSymbol(nameHint, type));
        }
        return symbolsBuilder.build();
    }

    private List<PlanNode> appendMarkers(List<Symbol> markers, List<PlanNode> nodes, SetOperationNode node)
    {
        ImmutableList.Builder<PlanNode> result = ImmutableList.builder();
        for (int i = 0; i < nodes.size(); i++) {
            result.add(appendMarkers(idAllocator, symbolAllocator, nodes.get(i), i, markers, node.sourceSymbolMap(i)));
        }
        return result.build();
    }

    private static PlanNode appendMarkers(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, PlanNode source, int markerIndex, List<Symbol> markers, Map<Symbol, SymbolReference> projections)
    {
        Assignments.Builder assignments = Assignments.builder();
        // add existing intersect symbols to projection
        for (Map.Entry<Symbol, SymbolReference> entry : projections.entrySet()) {
            Symbol symbol = symbolAllocator.newSymbol(entry.getKey().getName(), symbolAllocator.getTypes().get(entry.getKey()));
            assignments.put(symbol, entry.getValue());
        }

        // add extra marker fields to the projection
        for (int i = 0; i < markers.size(); ++i) {
            Expression expression = (i == markerIndex) ? TRUE_LITERAL : new Cast(new NullLiteral(), toSqlType(BOOLEAN));
            assignments.put(symbolAllocator.newSymbol(markers.get(i).getName(), BOOLEAN), expression);
        }

        return new ProjectNode(idAllocator.getNextId(), source, assignments.build());
    }

    private UnionNode union(List<PlanNode> nodes, List<Symbol> outputs)
    {
        ImmutableListMultimap.Builder<Symbol, Symbol> outputsToInputs = ImmutableListMultimap.builder();
        for (PlanNode source : nodes) {
            for (int i = 0; i < source.getOutputSymbols().size(); i++) {
                outputsToInputs.put(outputs.get(i), source.getOutputSymbols().get(i));
            }
        }

        return new UnionNode(idAllocator.getNextId(), nodes, outputsToInputs.build(), outputs);
    }

    private AggregationNode computeCounts(UnionNode sourceNode, List<Symbol> originalColumns, List<Symbol> markers, List<Symbol> aggregationOutputs)
    {
        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();

        for (int i = 0; i < markers.size(); i++) {
            Symbol output = aggregationOutputs.get(i);
            aggregations.put(output, new AggregationNode.Aggregation(
                    countFunction,
                    ImmutableList.of(markers.get(i).toSymbolReference()),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));
        }

        return new AggregationNode(idAllocator.getNextId(),
                sourceNode,
                aggregations.buildOrThrow(),
                singleGroupingSet(originalColumns),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());
    }

    private WindowNode appendCounts(UnionNode sourceNode, List<Symbol> originalColumns, List<Symbol> markers, List<Symbol> countOutputs, Symbol rowNumberSymbol)
    {
        ImmutableMap.Builder<Symbol, WindowNode.Function> functions = ImmutableMap.builder();
        WindowNode.Frame defaultFrame = new WindowNode.Frame(ROWS, UNBOUNDED_PRECEDING, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        for (int i = 0; i < markers.size(); i++) {
            Symbol output = countOutputs.get(i);
            functions.put(output, new WindowNode.Function(
                    countFunction,
                    ImmutableList.of(markers.get(i).toSymbolReference()),
                    defaultFrame,
                    false));
        }

        functions.put(rowNumberSymbol, new WindowNode.Function(
                rowNumberFunction,
                ImmutableList.of(),
                defaultFrame,
                false));

        return new WindowNode(
                idAllocator.getNextId(),
                sourceNode,
                new Specification(originalColumns, Optional.empty()),
                functions.buildOrThrow(),
                Optional.empty(),
                ImmutableSet.of(),
                0);
    }

    public static class TranslationResult
    {
        private final PlanNode planNode;
        private final List<Symbol> countSymbols;
        private final Optional<Symbol> rowNumberSymbol;

        public TranslationResult(PlanNode planNode, List<Symbol> countSymbols)
        {
            this(planNode, countSymbols, Optional.empty());
        }

        public TranslationResult(PlanNode planNode, List<Symbol> countSymbols, Optional<Symbol> rowNumberSymbol)
        {
            this.planNode = requireNonNull(planNode, "planNode is null");
            this.countSymbols = ImmutableList.copyOf(requireNonNull(countSymbols, "countSymbols is null"));
            this.rowNumberSymbol = requireNonNull(rowNumberSymbol, "rowNumberSymbol is null");
        }

        public PlanNode getPlanNode()
        {
            return this.planNode;
        }

        public List<Symbol> getCountSymbols()
        {
            return countSymbols;
        }

        public Symbol getRowNumberSymbol()
        {
            checkState(rowNumberSymbol.isPresent(), "rowNumberSymbol is empty");
            return rowNumberSymbol.get();
        }
    }
}
