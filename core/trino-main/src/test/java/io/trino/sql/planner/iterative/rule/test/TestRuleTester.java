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
package io.trino.sql.planner.iterative.rule.test;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.Rule.Context;
import io.trino.sql.planner.iterative.Rule.Result;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.iterative.rule.test.RuleTester.defaultRuleTester;
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

    @Test
    public void testReportNoFire()
    {
        try (RuleTester tester = defaultRuleTester()) {
            RuleAssert ruleAssert = tester.assertThat(
                    rule(
                            "testReportNoFire rule",
                            Pattern.typeOf(PlanNode.class),
                            (node, captures, context) -> Result.empty()))
                    .on(p ->
                            p.values(
                                    List.of(p.symbol("x")),
                                    List.of(List.of(expression("1")))));

            PlanMatchPattern expected = values(List.of("whatever"), List.of());
            assertThatThrownBy(() -> ruleAssert.matches(expected))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageMatching("testReportNoFire rule did not fire for:(?s:.*)");
        }
    }

    @Test
    public void testReportNoFireWithTableScan()
    {
        try (RuleTester tester = defaultRuleTester()) {
            RuleAssert ruleAssert = tester.assertThat(
                    rule(
                            "testReportNoFireWithTableScan rule",
                            Pattern.typeOf(PlanNode.class),
                            (node, captures, context) -> Result.empty()))
                    .on(p ->
                            p.tableScan(
                                    new TableHandle(tester.getCurrentCatalogHandle(), new TpchTableHandle("sf1", "nation", 1.0), TestingTransactionHandle.create()),
                                    List.of(p.symbol("x")),
                                    Map.of(p.symbol("x"), new TestingColumnHandle("column"))));

            PlanMatchPattern expected = values(List.of("whatever"), List.of());
            assertThatThrownBy(() -> ruleAssert.matches(expected))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageMatching("testReportNoFireWithTableScan rule did not fire for:\n" +
                            "(?s:.*)" +
                            "\\QEstimates: {rows: 25 (225B), cpu: 225, memory: 0B, network: 0B}\\E\n" +
                            "(?s:.*)");
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
