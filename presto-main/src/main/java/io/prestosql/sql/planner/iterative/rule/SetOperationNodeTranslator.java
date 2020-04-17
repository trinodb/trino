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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SetOperationNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.prestosql.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static java.util.Objects.requireNonNull;

public class SetOperationNodeTranslator
{
    private static final String MARKER = "marker";
    private static final Literal GENERIC_LITERAL = new GenericLiteral("BIGINT", "1");
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final ResolvedFunction countAggregation;

    public SetOperationNodeTranslator(Metadata metadata, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        this.symbolAllocator = requireNonNull(symbolAllocator, "SymbolAllocator is null");
        this.idAllocator = requireNonNull(idAllocator, "PlanNodeIdAllocator is null");
        requireNonNull(metadata, "metadata is null");
        this.countAggregation = metadata.resolveFunction(QualifiedName.of("count"), fromTypes(BOOLEAN));
    }

    public TranslationResult makeSetContainmentPlan(SetOperationNode node)
    {
        checkArgument(!(node instanceof UnionNode), "Cannot simplify a UnionNode");
        List<Symbol> markers = allocateSymbols(node.getSources().size(), MARKER, BOOLEAN);
        // identity projection for all the fields in each of the sources plus marker columns
        List<PlanNode> withMarkers = appendMarkers(markers, node.getSources(), node);

        // add a union over all the rewritten sources. The outputs of the union have the same name as the
        // original intersect node
        List<Symbol> outputs = node.getOutputSymbols();
        UnionNode union = union(withMarkers, ImmutableList.copyOf(concat(outputs, markers)));

        // add count aggregations and filter rows where any of the counts is >= 1
        List<Symbol> aggregationOutputs = allocateSymbols(markers.size(), "count", BIGINT);
        AggregationNode aggregation = computeCounts(union, outputs, markers, aggregationOutputs);
        List<Expression> presentExpression = aggregationOutputs.stream()
                .map(symbol -> new ComparisonExpression(GREATER_THAN_OR_EQUAL, symbol.toSymbolReference(), GENERIC_LITERAL))
                .collect(toImmutableList());
        return new TranslationResult(aggregation, presentExpression);
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
                    countAggregation,
                    ImmutableList.of(markers.get(i).toSymbolReference()),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));
        }

        return new AggregationNode(idAllocator.getNextId(),
                sourceNode,
                aggregations.build(),
                singleGroupingSet(originalColumns),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());
    }

    public static class TranslationResult
    {
        private final PlanNode planNode;
        private final List<Expression> presentExpressions;

        public TranslationResult(PlanNode planNode, List<Expression> presentExpressions)
        {
            this.planNode = requireNonNull(planNode, "AggregationNode is null");
            this.presentExpressions = ImmutableList.copyOf(requireNonNull(presentExpressions, "AggregationOutputs is null"));
        }

        public PlanNode getPlanNode()
        {
            return this.planNode;
        }

        public List<Expression> getPresentExpressions()
        {
            return presentExpressions;
        }
    }
}
