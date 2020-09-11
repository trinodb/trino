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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.metadata.FunctionId;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.optimizations.PlanNodeDecorrelator.DecorrelatedNode;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.CorrelatedJoinNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.QualifiedName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

// TODO: move this class to TransformCorrelatedScalarAggregationToJoin when old optimizer is gone
public class ScalarAggregationToJoinRewriter
{
    private final Metadata metadata;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Lookup lookup;
    private final PlanNodeDecorrelator planNodeDecorrelator;

    public ScalarAggregationToJoinRewriter(Metadata metadata, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.planNodeDecorrelator = new PlanNodeDecorrelator(metadata, symbolAllocator, lookup);
    }

    public PlanNode rewriteScalarAggregation(CorrelatedJoinNode correlatedJoinNode, AggregationNode aggregation)
    {
        List<Symbol> correlation = correlatedJoinNode.getCorrelation();
        Optional<DecorrelatedNode> source = planNodeDecorrelator.decorrelateFilters(aggregation.getSource(), correlation);
        if (source.isEmpty()) {
            return correlatedJoinNode;
        }

        Symbol nonNull = symbolAllocator.newSymbol("non_null", BooleanType.BOOLEAN);
        Assignments scalarAggregationSourceAssignments = Assignments.builder()
                .putIdentities(source.get().getNode().getOutputSymbols())
                .put(nonNull, TRUE_LITERAL)
                .build();
        ProjectNode scalarAggregationSourceWithNonNullableSymbol = new ProjectNode(
                idAllocator.getNextId(),
                source.get().getNode(),
                scalarAggregationSourceAssignments);

        return rewriteScalarAggregation(
                correlatedJoinNode,
                aggregation,
                scalarAggregationSourceWithNonNullableSymbol,
                source.get().getCorrelatedPredicates(),
                nonNull);
    }

    private PlanNode rewriteScalarAggregation(
            CorrelatedJoinNode correlatedJoinNode,
            AggregationNode scalarAggregation,
            PlanNode scalarAggregationSource,
            Optional<Expression> joinExpression,
            Symbol nonNull)
    {
        AssignUniqueId inputWithUniqueColumns = new AssignUniqueId(
                idAllocator.getNextId(),
                correlatedJoinNode.getInput(),
                symbolAllocator.newSymbol("unique", BigintType.BIGINT));

        JoinNode leftOuterJoin = new JoinNode(
                correlatedJoinNode.getId(),
                JoinNode.Type.LEFT,
                inputWithUniqueColumns,
                scalarAggregationSource,
                ImmutableList.of(),
                inputWithUniqueColumns.getOutputSymbols(),
                scalarAggregationSource.getOutputSymbols(),
                joinExpression,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        return createAggregationNode(
                scalarAggregation,
                leftOuterJoin,
                nonNull);
    }

    private AggregationNode createAggregationNode(
            AggregationNode scalarAggregation,
            JoinNode leftOuterJoin,
            Symbol nonNullableAggregationSourceSymbol)
    {
        FunctionId countFunctionId = metadata.resolveFunction(QualifiedName.of("count"), ImmutableList.of()).getFunctionId();
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : scalarAggregation.getAggregations().entrySet()) {
            Aggregation aggregation = entry.getValue();
            Symbol symbol = entry.getKey();
            // Only count() and count(*) require rewriting to count(non_null) in order to preserve the row count. count(argument) shouldn't be rewritten.
            if (aggregation.getResolvedFunction().getFunctionId().equals(countFunctionId)) {
                List<Type> scalarAggregationSourceTypes = ImmutableList.of(
                        symbolAllocator.getTypes().get(nonNullableAggregationSourceSymbol));
                aggregations.put(symbol, new Aggregation(
                        metadata.resolveFunction(QualifiedName.of("count"), fromTypes(scalarAggregationSourceTypes)),
                        ImmutableList.of(nonNullableAggregationSourceSymbol.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        aggregation.getMask()));
            }
            else {
                aggregations.put(symbol, aggregation);
            }
        }

        return new AggregationNode(
                scalarAggregation.getId(),
                leftOuterJoin,
                aggregations.build(),
                singleGroupingSet(leftOuterJoin.getLeft().getOutputSymbols()),
                ImmutableList.of(),
                scalarAggregation.getStep(),
                scalarAggregation.getHashSymbol(),
                Optional.empty());
    }
}
