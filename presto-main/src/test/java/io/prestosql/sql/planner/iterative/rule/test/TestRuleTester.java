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
package io.prestosql.sql.planner.iterative.rule.test;

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.Rule.Context;
import io.prestosql.sql.planner.iterative.Rule.Result;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.PlanNode;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.planner.iterative.rule.test.RuleTester.defaultRuleTester;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRuleTester
{
    @Test
    public void testReportWrongMatch()
    {
        try (RuleTester tester = defaultRuleTester()) {
            RuleAssert ruleAssert = tester.assertThat(
                    rule(
                            "testReportWrongMatch rule",
                            Pattern.typeOf(PlanNode.class),
                            (node, captures, context) -> Result.ofPlanNode(node.replaceChildren(node.getSources()))))
                    .on(p ->
                            p.project(
                                    Assignments.of(p.symbol("y"), expression("x")),
                                    p.values(
                                            ImmutableList.of(p.symbol("x")),
                                            ImmutableList.of(ImmutableList.of(expression("1"))))));

            PlanMatchPattern expected = values(ImmutableList.of("different"), ImmutableList.of());
            assertThatThrownBy(() -> ruleAssert.matches(expected))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageMatching("(?s)Plan does not match, expected .* but found .*");
        }
    }

    private static <T> Rule<T> rule(String name, Pattern<T> pattern, RuleApplyImplementation<T> apply)
    {
        requireNonNull(name, "name is null");
        requireNonNull(pattern, "pattern is null");
        requireNonNull(apply, "apply is null");
        return new Rule<T>()
        {
            @Override
            public String toString()
            {
                return name;
            }

            @Override
            public Pattern<T> getPattern()
            {
                return pattern;
            }

            @Override
            public Result apply(T node, Captures captures, Context context)
            {
                return apply.apply(node, captures, context);
            }
        };
    }

    @FunctionalInterface
    private interface RuleApplyImplementation<T>
    {
        Result apply(T node, Captures captures, Context context);
    }
}
