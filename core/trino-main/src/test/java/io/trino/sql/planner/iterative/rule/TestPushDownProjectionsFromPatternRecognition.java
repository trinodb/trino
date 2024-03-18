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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.rowpattern.AggregatedSetDescriptor;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.ClassifierValuePointer;
import io.trino.sql.planner.rowpattern.LogicalIndexPointer;
import io.trino.sql.planner.rowpattern.MatchNumberValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.patternRecognition;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushDownProjectionsFromPatternRecognition
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction MULTIPLY_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(INTEGER, INTEGER));

    private static final ResolvedFunction CONCAT = FUNCTIONS.resolveFunction("concat", fromTypes(VARCHAR, VARCHAR));
    private static final ResolvedFunction MAX_BY = createTestMetadataManager().resolveBuiltinFunction("max_by", fromTypes(BIGINT, BIGINT));

    @Test
    public void testNoAggregations()
    {
        tester().assertThat(new PushDownProjectionsFromPatternRecognition())
                .on(p -> p.patternRecognition(builder -> builder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testDoNotPushRuntimeEvaluatedArguments()
    {
        tester().assertThat(new PushDownProjectionsFromPatternRecognition())
                .on(p -> p.patternRecognition(builder -> builder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(
                                new IrLabel("X"),
                                new ComparisonExpression(GREATER_THAN, new FunctionCall(MAX_BY, ImmutableList.of(
                                        new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new Constant(INTEGER, 1L), new SymbolReference("match")),
                                        new FunctionCall(CONCAT, ImmutableList.of(new Constant(VARCHAR, Slices.utf8Slice("x")), new SymbolReference("classifier"))))),
                                        new Constant(INTEGER, 5L)),
                                ImmutableMap.of(
                                        "classifier", new ClassifierValuePointer(new LogicalIndexPointer(ImmutableSet.of(), true, true, 0, 0)),
                                        "match", new MatchNumberValuePointer()))
                        .source(p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testDoNotPushSymbolReferences()
    {
        tester().assertThat(new PushDownProjectionsFromPatternRecognition())
                .on(p -> p.patternRecognition(builder -> builder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(
                                new IrLabel("X"),
                                new ComparisonExpression(GREATER_THAN, new FunctionCall(MAX_BY, ImmutableList.of(new SymbolReference("a"), new SymbolReference("b"))), new Constant(INTEGER, 5L)))
                        .source(p.values(p.symbol("a"), p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testPreProjectArguments()
    {
        ResolvedFunction maxBy = tester().getMetadata().resolveBuiltinFunction("max_by", fromTypes(BIGINT, BIGINT));
        tester().assertThat(new PushDownProjectionsFromPatternRecognition())
                .on(p -> p.patternRecognition(builder -> builder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(
                                new IrLabel("X"),
                                new ComparisonExpression(LESS_THAN, new SymbolReference("agg"), new Constant(INTEGER, 5L)),
                                ImmutableMap.of("agg", new AggregationValuePointer(
                                        maxBy,
                                        new AggregatedSetDescriptor(ImmutableSet.of(), true),
                                        ImmutableList.of(new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new SymbolReference("a"), new Constant(INTEGER, 1L)), new ArithmeticBinaryExpression(MULTIPLY_INTEGER, MULTIPLY, new SymbolReference("b"), new Constant(INTEGER, 2L))),
                                        Optional.empty(),
                                        Optional.empty())))
                        .source(p.values(p.symbol("a"), p.symbol("b")))))
                .matches(
                        patternRecognition(builder -> builder
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(
                                                new IrLabel("X"),
                                                new ComparisonExpression(LESS_THAN, new SymbolReference("agg"), new Constant(INTEGER, 5L)),
                                                ImmutableMap.of("agg", new AggregationValuePointer(
                                                        maxBy,
                                                        new AggregatedSetDescriptor(ImmutableSet.of(), true),
                                                        ImmutableList.of(new SymbolReference("expr_1"), new SymbolReference("expr_2")),
                                                        Optional.empty(),
                                                        Optional.empty()))),
                                project(
                                        ImmutableMap.of(
                                                "expr_1", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new SymbolReference("a"), new Constant(INTEGER, 1L))),
                                                "expr_2", PlanMatchPattern.expression(new ArithmeticBinaryExpression(MULTIPLY_INTEGER, MULTIPLY, new SymbolReference("b"), new Constant(INTEGER, 2L))),
                                                "a", PlanMatchPattern.expression(new SymbolReference("a")),
                                                "b", PlanMatchPattern.expression(new SymbolReference("b"))),
                                        values("a", "b"))));
    }
}
