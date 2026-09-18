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

import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.rowpattern.AggregatedSetDescriptor;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.optimizations.SymbolMapper.symbolMapper;
import static io.trino.sql.planner.optimizations.SymbolMapper.symbolReallocator;
import static io.trino.sql.planner.plan.RowsPerMatch.ONE;
import static io.trino.sql.planner.plan.SkipToPosition.PAST_LAST;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSymbolMapper
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();

    @Test
    public void testReallocateSymbolsInLetKeepsBinderAndBodyConsistent()
    {
        Symbol input = new Symbol(VARCHAR, "input");
        Symbol bound = new Symbol(VARCHAR, "bound");
        Map<Symbol, Symbol> mapping = new HashMap<>();
        // Seed the allocator so that reallocation actually renames the symbols being mapped.
        SymbolMapper mapper = symbolReallocator(mapping, new SymbolAllocator(List.of(input, bound)));

        Expression mapped = mapper.map(new Let(bound, input.toSymbolReference(), bound.toSymbolReference()));

        assertThat(mapped).isInstanceOf(Let.class);
        Let let = (Let) mapped;

        // The binder is reallocated together with the references to it.
        assertThat(let.name()).isNotEqualTo(bound);
        assertThat(let.body()).isEqualTo(let.name().toSymbolReference());

        // Only the free symbol is a dependency; the bound one must not leak out.
        assertThat(SymbolsExtractor.extractUnique(let)).containsExactly(mapper.map(input));
    }

    @Test
    public void testReallocateSymbolsInNestedLet()
    {
        Symbol input = new Symbol(VARCHAR, "input");
        Symbol outer = new Symbol(VARCHAR, "outer");
        Symbol inner = new Symbol(VARCHAR, "inner");
        Map<Symbol, Symbol> mapping = new HashMap<>();
        SymbolMapper mapper = symbolReallocator(mapping, new SymbolAllocator(List.of(input, outer, inner)));

        Let let = (Let) mapper.map(new Let(
                outer,
                input.toSymbolReference(),
                new Let(inner, outer.toSymbolReference(), inner.toSymbolReference())));

        Let nested = (Let) let.body();
        assertThat(let.name()).isNotEqualTo(outer);
        assertThat(nested.name()).isNotEqualTo(inner);
        assertThat(nested.value()).isEqualTo(let.name().toSymbolReference());
        assertThat(nested.body()).isEqualTo(nested.name().toSymbolReference());
        assertThat(SymbolsExtractor.extractUnique(let)).containsExactly(mapper.map(input));
    }

    @Test
    public void testReallocateSymbolsInLambdaWithConstantBody()
    {
        Symbol argument = new Symbol(VARCHAR, "argument");
        Constant body = new Constant(BIGINT, 1L);
        Map<Symbol, Symbol> mapping = new HashMap<>();
        SymbolMapper mapper = symbolReallocator(mapping, new SymbolAllocator(List.of(argument)));

        Lambda lambda = (Lambda) mapper.map(new Lambda(List.of(argument), body));

        assertThat(lambda.arguments()).containsExactly(mapper.map(argument));
        assertThat(lambda.arguments()).doesNotContain(argument);
        assertThat(lambda.body()).isSameAs(body);
    }

    @Test
    public void testReallocateSymbolsInAggregationValuePointer()
    {
        Symbol input = new Symbol(BIGINT, "input");
        Symbol bound = new Symbol(BIGINT, "bound");
        Symbol classifier = new Symbol(VARCHAR, "classifier");
        Symbol matchNumber = new Symbol(BIGINT, "match_number");
        Symbol aggregationResult = new Symbol(BIGINT, "aggregation_result");
        IrLabel label = new IrLabel("A");

        Expression argument = new Let(
                bound,
                input.toSymbolReference(),
                new Case(
                        List.of(new WhenClause(new IsNull(classifier.toSymbolReference()), bound.toSymbolReference())),
                        matchNumber.toSymbolReference()));
        AggregationValuePointer pointer = new AggregationValuePointer(
                FUNCTIONS.resolveFunction("max", fromTypes(BIGINT)),
                new AggregatedSetDescriptor(Set.of(label), false),
                List.of(argument),
                Optional.of(classifier),
                Optional.of(matchNumber));
        Expression predicate = new IsNull(aggregationResult.toSymbolReference());
        ExpressionAndValuePointers definition = new ExpressionAndValuePointers(
                predicate,
                List.of(new ExpressionAndValuePointers.Assignment(aggregationResult, pointer)));

        ValuesNode source = new ValuesNode(new PlanNodeId("source"), List.of(input));
        PatternRecognitionNode node = new PatternRecognitionNode(
                new PlanNodeId("pattern"),
                source,
                new DataOrganizationSpecification(List.of(), Optional.empty()),
                Set.of(),
                0,
                Map.of(),
                Map.of(),
                Optional.empty(),
                ONE,
                Set.of(),
                PAST_LAST,
                true,
                label,
                Map.of(label, definition));

        Map<Symbol, Symbol> mapping = new HashMap<>();
        // Seed every original symbol so all expected remappings are observable.
        SymbolMapper mapper = symbolReallocator(
                mapping,
                new SymbolAllocator(List.of(input, bound, classifier, matchNumber, aggregationResult)));
        Symbol mappedInput = mapper.map(input);
        ValuesNode mappedSource = new ValuesNode(source.getId(), List.of(mappedInput));

        PatternRecognitionNode mappedNode = mapper.map(node, mappedSource);
        ExpressionAndValuePointers mappedDefinition = mappedNode.getVariableDefinitions().get(label);
        ExpressionAndValuePointers.Assignment mappedAssignment = mappedDefinition.getAssignments().getFirst();
        AggregationValuePointer mappedPointer = (AggregationValuePointer) mappedAssignment.valuePointer();
        Let mappedArgument = (Let) mappedPointer.getArguments().getFirst();
        Case mappedBody = (Case) mappedArgument.body();
        Symbol mappedClassifier = mappedPointer.getClassifierSymbol().orElseThrow();
        Symbol mappedMatchNumber = mappedPointer.getMatchNumberSymbol().orElseThrow();

        // The synthetic assignment and its top-level expression belong to this definition and stay local.
        assertThat(mappedDefinition.getExpression()).isEqualTo(predicate);
        assertThat(mappedAssignment.symbol()).isEqualTo(aggregationResult);

        assertThat(mappedArgument.name()).isNotEqualTo(bound);
        assertThat(mappedArgument.value()).isEqualTo(mappedInput.toSymbolReference());
        assertThat(mappedBody.whenClauses().getFirst().getResult()).isEqualTo(mappedArgument.name().toSymbolReference());

        assertThat(mappedClassifier).isNotEqualTo(classifier);
        assertThat(((IsNull) mappedBody.whenClauses().getFirst().getOperand()).value()).isEqualTo(mappedClassifier.toSymbolReference());
        assertThat(mappedMatchNumber).isNotEqualTo(matchNumber);
        assertThat(mappedBody.defaultValue()).isEqualTo(mappedMatchNumber.toSymbolReference());

        assertThat(mappedDefinition.getInputSymbols()).containsExactly(mappedInput);
    }

    @Test
    public void testCanonicalizeLeavesLetBinderAloneWhenNotMapped()
    {
        Symbol from = new Symbol(VARCHAR, "from");
        Symbol to = new Symbol(VARCHAR, "to");
        Symbol bound = new Symbol(VARCHAR, "bound");
        SymbolMapper mapper = symbolMapper(Map.of(from, to));

        Let let = (Let) mapper.map(new Let(bound, from.toSymbolReference(), bound.toSymbolReference()));

        // The binder is unchanged because it is not a key in the canonicalization map.
        assertThat(let.name()).isEqualTo(bound);
        assertThat(let.value()).isEqualTo(to.toSymbolReference());
        assertThat(let.body()).isEqualTo(bound.toSymbolReference());
    }
}
