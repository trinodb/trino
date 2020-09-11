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
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.assertions.TopNRowNumberSymbolMatcher;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.WindowNode.Specification;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.topNRowNumber;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneTopNRowNumberColumns
        extends BaseRuleTest
{
    @Test
    public void testDoNotPrunePartitioningSymbol()
    {
        tester().assertThat(new PruneTopNRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(b, rowNumber),
                            p.topNRowNumber(
                                    new Specification(
                                            ImmutableList.of(a),
                                            Optional.of(new OrderingScheme(ImmutableList.of(b), ImmutableMap.of(b, SortOrder.ASC_NULLS_FIRST)))),
                                    5,
                                    rowNumber,
                                    Optional.empty(),
                                    p.values(a, b)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneOrderingSymbol()
    {
        tester().assertThat(new PruneTopNRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(rowNumber),
                            p.topNRowNumber(
                                    new Specification(
                                            ImmutableList.of(),
                                            Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST)))),
                                    5,
                                    rowNumber,
                                    Optional.empty(),
                                    p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneHashSymbol()
    {
        tester().assertThat(new PruneTopNRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol hash = p.symbol("hash");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(a, rowNumber),
                            p.topNRowNumber(
                                    new Specification(
                                            ImmutableList.of(),
                                            Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST)))),
                                    5,
                                    rowNumber,
                                    Optional.of(hash),
                                    p.values(a, hash)));
                })
                .doesNotFire();
    }

    @Test
    public void testSourceSymbolNotReferenced()
    {
        tester().assertThat(new PruneTopNRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(a, rowNumber),
                            p.topNRowNumber(
                                    new Specification(
                                            ImmutableList.of(),
                                            Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST)))),
                                    5,
                                    rowNumber,
                                    Optional.empty(),
                                    p.values(a, b)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", expression("a"), "row_number", expression("row_number")),
                                topNRowNumber(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of(),
                                                        ImmutableList.of("a"),
                                                        ImmutableMap.of("a", SortOrder.ASC_NULLS_FIRST))
                                                .maxRowCountPerPartition(5),
                                        strictProject(
                                                ImmutableMap.of("a", expression("a")),
                                                values("a", "b")))
                                        .withAlias("row_number", new TopNRowNumberSymbolMatcher())));
    }

    @Test
    public void testAllSymbolsReferenced()
    {
        tester().assertThat(new PruneTopNRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(a, b, rowNumber),
                            p.topNRowNumber(
                                    new Specification(
                                            ImmutableList.of(),
                                            Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST)))),
                                    5,
                                    rowNumber,
                                    Optional.empty(),
                                    p.values(a, b)));
                })
                .doesNotFire();
    }

    @Test
    public void testRowNumberSymbolNotReferenced()
    {
        tester().assertThat(new PruneTopNRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(a),
                            p.topNRowNumber(
                                    new Specification(
                                            ImmutableList.of(),
                                            Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST)))),
                                    5,
                                    rowNumber,
                                    Optional.empty(),
                                    p.values(a)));
                })
                .doesNotFire();
    }
}
