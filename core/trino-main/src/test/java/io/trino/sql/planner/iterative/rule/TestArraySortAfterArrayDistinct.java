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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.ArrayType;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.StringLiteral;
import io.trino.type.FunctionType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestArraySortAfterArrayDistinct
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ARRAY = FUNCTIONS.resolveFunction("$array", fromTypes(new ArrayType(VARCHAR)));
    private static final ResolvedFunction SORT = FUNCTIONS.resolveFunction("array_sort", fromTypes(new ArrayType(VARCHAR)));
    private static final ResolvedFunction SORT_WITH_LAMBDA = FUNCTIONS.resolveFunction("array_sort", fromTypes(new ArrayType(VARCHAR), new FunctionType(ImmutableList.of(VARCHAR, VARCHAR), INTEGER)));
    private static final ResolvedFunction DISTINCT = FUNCTIONS.resolveFunction("array_distinct", fromTypes(new ArrayType(VARCHAR)));

    @Test
    public void testArrayDistinctAfterArraySort()
    {
        test(
                new FunctionCall(DISTINCT.toQualifiedName(), ImmutableList.of(new FunctionCall(SORT.toQualifiedName(), ImmutableList.of(new FunctionCall(ARRAY.toQualifiedName(), ImmutableList.of(new StringLiteral("a"))))))),
                new FunctionCall(SORT.toQualifiedName(), ImmutableList.of(new FunctionCall(DISTINCT.toQualifiedName(), ImmutableList.of(new FunctionCall(ARRAY.toQualifiedName(), ImmutableList.of(new StringLiteral("a"))))))));
    }

    @Test
    public void testArrayDistinctAfterArraySortWithLambda()
    {
        test(
                new FunctionCall(DISTINCT.toQualifiedName(), ImmutableList.of(
                        new FunctionCall(SORT_WITH_LAMBDA.toQualifiedName(), ImmutableList.of(
                                new FunctionCall(ARRAY.toQualifiedName(), ImmutableList.of(new StringLiteral("a"))),
                                new LambdaExpression(ImmutableList.of(
                                        new LambdaArgumentDeclaration(new Identifier("a")),
                                        new LambdaArgumentDeclaration(new Identifier("b"))),
                                        new LongLiteral("1")))))),
                new FunctionCall(SORT_WITH_LAMBDA.toQualifiedName(), ImmutableList.of(
                        new FunctionCall(DISTINCT.toQualifiedName(), ImmutableList.of(
                                new FunctionCall(ARRAY.toQualifiedName(), ImmutableList.of(new StringLiteral("a"))))),
                        new LambdaExpression(ImmutableList.of(
                                new LambdaArgumentDeclaration(new Identifier("a")),
                                new LambdaArgumentDeclaration(new Identifier("b"))),
                                new LongLiteral("1")))));
    }

    private void test(Expression original, Expression rewritten)
    {
        tester().assertThat(new ArraySortAfterArrayDistinct(tester().getPlannerContext()).projectExpressionRewrite())
                .on(p -> p.project(
                        Assignments.builder()
                                .put(p.symbol("output"), original)
                                .build(),
                        p.values()))
                .matches(
                        project(Map.of("output", PlanMatchPattern.expression(rewritten)),
                                values()));
    }
}
