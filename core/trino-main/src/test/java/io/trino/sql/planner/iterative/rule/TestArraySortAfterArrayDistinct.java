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

import io.trino.sql.ExpressionTestUtils;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.Expression;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestArraySortAfterArrayDistinct
        extends BaseRuleTest
{
    @Test
    public void testArrayDistinctAfterArraySort()
    {
        test("ARRAY_DISTINCT(ARRAY_SORT(\"$array\"('a')))", "ARRAY_SORT(ARRAY_DISTINCT(\"$array\"('a')))");
    }

    @Test
    public void testArrayDistinctAfterArraySortWithLambda()
    {
        test("ARRAY_DISTINCT(ARRAY_SORT(\"$array\"('a'), (a, b) -> 1))", "ARRAY_SORT(ARRAY_DISTINCT(\"$array\"('a')), (a, b) -> 1)");
    }

    private void test(String original, String rewritten)
    {
        tester().assertThat(new ArraySortAfterArrayDistinct(tester().getPlannerContext()).projectExpressionRewrite())
                .on(p -> p.project(
                        Assignments.builder()
                                .put(p.symbol("output"), expression(original))
                                .build(),
                        p.values()))
                .matches(
                        project(Map.of("output", PlanMatchPattern.expression(expression(rewritten))),
                                values()));
    }

    private Expression expression(String sql)
    {
        return ExpressionTestUtils.planExpression(tester().getPlannerContext(), tester().getSession(), TypeProvider.empty(), PlanBuilder.expression(sql));
    }
}
