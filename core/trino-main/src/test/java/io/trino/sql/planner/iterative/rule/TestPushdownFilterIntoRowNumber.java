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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPushdownFilterIntoRowNumber
        extends BaseRuleTest
{
    @Test
    public void testSourceRowNumber()
    {
        tester().assertThat(new PushdownFilterIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(
                            expression("row_number_1 < cast(100 as bigint)"),
                            p.rowNumber(
                                    ImmutableList.of(a),
                                    Optional.empty(),
                                    rowNumberSymbol,
                                    p.values(a)));
                })
                .matches(
                        rowNumber(rowNumber -> rowNumber
                                        .maxRowCountPerPartition(Optional.of(99))
                                        .partitionBy(ImmutableList.of("a")),
                                values("a")));

        tester().assertThat(new PushdownFilterIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(
                            expression("row_number_1 < cast(100 as bigint)"),
                            p.rowNumber(
                                    ImmutableList.of(a),
                                    Optional.of(10),
                                    rowNumberSymbol,
                                    p.values(a)));
                })
                .matches(
                        rowNumber(rowNumber -> rowNumber
                                        .maxRowCountPerPartition(Optional.of(10))
                                        .partitionBy(ImmutableList.of("a")),
                                values("a")));

        tester().assertThat(new PushdownFilterIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(
                            expression("cast(3 as bigint) < row_number_1 and row_number_1 < cast(5 as bigint)"),
                            p.rowNumber(
                                    ImmutableList.of(a),
                                    Optional.of(10),
                                    rowNumberSymbol,
                                    p.values(a)));
                })
                .matches(
                        filter(
                                "cast(3 as bigint) < row_number_1 and row_number_1 < cast(5 as bigint)",
                                rowNumber(rowNumber -> rowNumber
                                                .maxRowCountPerPartition(Optional.of(4))
                                                .partitionBy(ImmutableList.of("a")),
                                        values("a"))
                                        .withAlias("row_number_1", new RowNumberSymbolMatcher())));

        tester().assertThat(new PushdownFilterIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(
                            expression("row_number_1 < cast(5 as bigint) and a = BIGINT '1'"),
                            p.rowNumber(
                                    ImmutableList.of(a),
                                    Optional.of(10),
                                    rowNumberSymbol,
                                    p.values(a)));
                })
                .matches(
                        filter(
                                "a = BIGINT '1'",
                                rowNumber(rowNumber -> rowNumber
                                                .maxRowCountPerPartition(Optional.of(4))
                                                .partitionBy(ImmutableList.of("a")),
                                        values("a"))
                                        .withAlias("row_number_1", new RowNumberSymbolMatcher())));
    }

    @Test
    public void testNoOutputsThroughRowNumber()
    {
        tester().assertThat(new PushdownFilterIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(expression("row_number_1 < cast(-100 as bigint)"),
                            p.rowNumber(ImmutableList.of(p.symbol("a")), Optional.empty(), rowNumberSymbol,
                                    p.values(p.symbol("a"))));
                })
                .matches(values("a", "row_number_1"));
    }

    @Test
    public void testDoNotFire()
    {
        tester().assertThat(new PushdownFilterIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(expression("not_row_number < cast(100 as bigint)"),
                            p.rowNumber(ImmutableList.of(p.symbol("a")), Optional.empty(), rowNumberSymbol,
                                    p.values(p.symbol("a"), p.symbol("not_row_number"))));
                })
                .doesNotFire();

        tester().assertThat(new PushdownFilterIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(expression("row_number_1 > cast(100 as bigint)"),
                            p.rowNumber(ImmutableList.of(p.symbol("a")), Optional.empty(), rowNumberSymbol,
                                    p.values(p.symbol("a"))));
                })
                .doesNotFire();

        tester().assertThat(new PushdownFilterIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(
                            expression("cast(3 as bigint) < row_number_1 and row_number_1 < cast(5 as bigint)"),
                            p.rowNumber(
                                    ImmutableList.of(a),
                                    Optional.of(4),
                                    rowNumberSymbol,
                                    p.values(a)));
                })
                .doesNotFire();
    }
}
