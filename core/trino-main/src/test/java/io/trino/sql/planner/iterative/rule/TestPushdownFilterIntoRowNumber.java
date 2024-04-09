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
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

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
                            new Comparison(LESS_THAN, new Reference(BIGINT, "row_number_1"), new Constant(BIGINT, 100L)),
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
                            new Comparison(LESS_THAN, new Reference(BIGINT, "row_number_1"), new Constant(BIGINT, 100L)),
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
                            new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Constant(BIGINT, 3L), new Reference(BIGINT, "row_number_1")), new Comparison(LESS_THAN, new Reference(BIGINT, "row_number_1"), new Constant(BIGINT, 5L)))),
                            p.rowNumber(
                                    ImmutableList.of(a),
                                    Optional.of(10),
                                    rowNumberSymbol,
                                    p.values(a)));
                })
                .matches(
                        filter(
                                new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Constant(BIGINT, 3L), new Reference(BIGINT, "row_number_1")), new Comparison(LESS_THAN, new Reference(BIGINT, "row_number_1"), new Constant(BIGINT, 5L)))),
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
                            new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(BIGINT, "row_number_1"), new Constant(BIGINT, 5L)), new Comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)))),
                            p.rowNumber(
                                    ImmutableList.of(a),
                                    Optional.of(10),
                                    rowNumberSymbol,
                                    p.values(a)));
                })
                .matches(
                        filter(
                                new Comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)),
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
                    return p.filter(
                            new Comparison(LESS_THAN, new Reference(BIGINT, "row_number_1"), new Constant(BIGINT, -100L)),
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
                    return p.filter(
                            new Comparison(LESS_THAN, new Reference(BIGINT, "not_row_number"), new Cast(new Constant(INTEGER, 100L), BIGINT)),
                            p.rowNumber(ImmutableList.of(p.symbol("a")), Optional.empty(), rowNumberSymbol,
                                    p.values(p.symbol("a"), p.symbol("not_row_number"))));
                })
                .doesNotFire();

        tester().assertThat(new PushdownFilterIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(
                            new Comparison(GREATER_THAN, new Reference(BIGINT, "row_number_1"), new Cast(new Constant(INTEGER, 100L), BIGINT)),
                            p.rowNumber(ImmutableList.of(p.symbol("a")), Optional.empty(), rowNumberSymbol,
                                    p.values(p.symbol("a"))));
                })
                .doesNotFire();

        tester().assertThat(new PushdownFilterIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Cast(new Constant(INTEGER, 3L), BIGINT), new Reference(BIGINT, "row_number_1")), new Comparison(LESS_THAN, new Reference(BIGINT, "row_number_1"), new Cast(new Constant(INTEGER, 5L), BIGINT)))),
                            p.rowNumber(
                                    ImmutableList.of(a),
                                    Optional.of(4),
                                    rowNumberSymbol,
                                    p.values(a)));
                })
                .doesNotFire();
    }
}
