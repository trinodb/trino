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
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.patternRecognition;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;

public class TestPushDownProjectionsFromPatternRecognition
        extends BaseRuleTest
{
    private static final QualifiedName MAX_BY = createTestMetadataManager().resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(BIGINT, BIGINT)).toQualifiedName();

    @Test
    public void testNoAggregations()
    {
        tester().assertThat(new PushDownProjectionsFromPatternRecognition())
                .on(p -> p.patternRecognition(builder -> builder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
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
                                new ComparisonExpression(GREATER_THAN, new FunctionCall(MAX_BY, ImmutableList.of(expression("1 + MATCH_NUMBER()"), expression("'x' || CLASSIFIER()"))), expression("5")))
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
                                new ComparisonExpression(GREATER_THAN, new FunctionCall(MAX_BY, ImmutableList.of(expression("a"), expression("b"))), expression("5")))
                        .source(p.values(p.symbol("a"), p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testPreProjectArguments()
    {
        tester().assertThat(new PushDownProjectionsFromPatternRecognition())
                .on(p -> p.patternRecognition(builder -> builder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(
                                new IrLabel("X"),
                                new ComparisonExpression(GREATER_THAN, new FunctionCall(MAX_BY, ImmutableList.of(expression("a + 1"), expression("b * 2"))), expression("5")))
                        .source(p.values(p.symbol("a"), p.symbol("b")))))
                .matches(
                        patternRecognition(builder -> builder
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(
                                                new IrLabel("X"),
                                                new ComparisonExpression(GREATER_THAN, new FunctionCall(MAX_BY, ImmutableList.of(expression("expr_1"), expression("expr_2"))), expression("5"))),
                                project(
                                        ImmutableMap.of(
                                                "expr_1", PlanMatchPattern.expression("a + 1"),
                                                "expr_2", PlanMatchPattern.expression("b * 2"),
                                                "a", PlanMatchPattern.expression("a"),
                                                "b", PlanMatchPattern.expression("b")),
                                        values("a", "b"))));
    }
}
