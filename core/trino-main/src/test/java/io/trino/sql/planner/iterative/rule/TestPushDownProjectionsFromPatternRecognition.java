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
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.rowpattern.AggregatedSetDescriptor;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.patternRecognition;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;

public class TestPushDownProjectionsFromPatternRecognition
        extends BaseRuleTest
{
    private static final QualifiedName MAX_BY = createTestMetadataManager().resolveBuiltinFunction("max_by", fromTypes(BIGINT, BIGINT)).toQualifiedName();

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
                                        new ArithmeticBinaryExpression(ADD, new LongLiteral("1"), new FunctionCall(QualifiedName.of("match_number"), ImmutableList.of())),
                                        new FunctionCall(QualifiedName.of("concat"), ImmutableList.of(new StringLiteral("x"), new FunctionCall(QualifiedName.of("classifier"), ImmutableList.of()))))),
                                        new LongLiteral("5")))
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
                                new ComparisonExpression(GREATER_THAN, new FunctionCall(MAX_BY, ImmutableList.of(new SymbolReference("a"), new SymbolReference("b"))), new LongLiteral("5")))
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
                                new ComparisonExpression(LESS_THAN, new SymbolReference("agg"), new LongLiteral("5")),
                                ImmutableMap.of("agg", new AggregationValuePointer(
                                        maxBy,
                                        new AggregatedSetDescriptor(ImmutableSet.of(), true),
                                        ImmutableList.of(new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new LongLiteral("1")), new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("b"), new LongLiteral("2"))),
                                        Optional.empty(),
                                        Optional.empty())))
                        .source(p.values(p.symbol("a"), p.symbol("b")))))
                .matches(
                        patternRecognition(builder -> builder
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(
                                                new IrLabel("X"),
                                                new ComparisonExpression(LESS_THAN, new SymbolReference("agg"), new LongLiteral("5")),
                                                ImmutableMap.of("agg", new AggregationValuePointer(
                                                        maxBy,
                                                        new AggregatedSetDescriptor(ImmutableSet.of(), true),
                                                        ImmutableList.of(new SymbolReference("expr_1"), new SymbolReference("expr_2")),
                                                        Optional.empty(),
                                                        Optional.empty()))),
                                project(
                                        ImmutableMap.of(
                                                "expr_1", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new LongLiteral("1"))),
                                                "expr_2", PlanMatchPattern.expression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("b"), new LongLiteral("2"))),
                                                "a", PlanMatchPattern.expression(new SymbolReference("a")),
                                                "b", PlanMatchPattern.expression(new SymbolReference("b"))),
                                        values("a", "b"))));
    }
}
