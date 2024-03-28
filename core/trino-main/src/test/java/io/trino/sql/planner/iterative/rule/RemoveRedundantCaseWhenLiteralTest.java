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
import io.airlift.slice.Slices;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.NotExpression;
import io.trino.sql.ir.SearchedCaseExpression;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.util.DateTimeUtils;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.LogicalExpression.Operator.OR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class RemoveRedundantCaseWhenLiteralTest
        extends BaseRuleTest
{
    private static final Constant ZERO = new Constant(INTEGER, 0L);
    private static final Constant ONE = new Constant(INTEGER, 1L);

    @Test
    public void testNumbers()
    {
        // CASE WHEN a = 1 THEN 1 ELSE 0 END = 1
        tester()
                .assertThat(new RemoveRedundantCaseWhenLiteral(tester().getPlannerContext(), tester().getTypeAnalyzer()))
                .on(p -> p.filter(
                        new ComparisonExpression(
                                EQUAL,
                                new SearchedCaseExpression(
                                        ImmutableList.of(new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), ONE), ONE)),
                                        Optional.of(ZERO)),
                                new Constant(INTEGER, 1L)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new ComparisonExpression(EQUAL, new SymbolReference("a"), ONE),
                                values("a")));

        // CASE WHEN a = 1 THEN 1 ELSE 0 END <> 0
        tester()
                .assertThat(new RemoveRedundantCaseWhenLiteral(tester().getPlannerContext(), tester().getTypeAnalyzer()))
                .on(p -> p.filter(
                        new ComparisonExpression(
                                ComparisonExpression.Operator.NOT_EQUAL,
                                new SearchedCaseExpression(
                                        ImmutableList.of(new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), ONE), ONE)),
                                        Optional.of(ZERO)),
                                new Constant(INTEGER, 1L)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("a"), ONE)),
                                values("a")));

        // CASE WHEN a = 1 THEN 1 ELSE 0 END = 0
        tester()
                .assertThat(new RemoveRedundantCaseWhenLiteral(tester().getPlannerContext(), tester().getTypeAnalyzer()))
                .on(p -> p.filter(
                        new ComparisonExpression(
                                EQUAL,
                                new SearchedCaseExpression(
                                        ImmutableList.of(new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), ONE), ONE)),
                                        Optional.of(ZERO)),
                                ZERO),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("a"), ONE)),
                   values("a")));

        // CASE WHEN a = 1 THEN 1 WHEN a = 2 THEN 2 WHEN a = 3 THEN 3 ELSE 0 END = 2
        tester()
                .assertThat(new RemoveRedundantCaseWhenLiteral(tester().getPlannerContext(), tester().getTypeAnalyzer()))
                .on(p -> p.filter(
                        new ComparisonExpression(
                                EQUAL,
                                new SearchedCaseExpression(
                                        ImmutableList.of(
                                                new WhenClause(aEqualsTo(1), ONE),
                                                new WhenClause(aEqualsTo(2), new Constant(INTEGER, 2L)),
                                                new WhenClause(aEqualsTo(3), new Constant(INTEGER, 3L)),
                                                new WhenClause(aEqualsTo(4), new Constant(INTEGER, 4L))),
                                        Optional.of(ZERO)),
                                new Constant(INTEGER, 2L)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(aEqualsTo(2L), values("a")));

        // CASE WHEN a = 1 THEN 1 WHEN a = 2 THEN 1 WHEN a = 3 THEN 1 ELSE 0 END = 1
        tester()
                .assertThat(new RemoveRedundantCaseWhenLiteral(tester().getPlannerContext(), tester().getTypeAnalyzer()))
                .on(p -> p.filter(
                        new ComparisonExpression(
                                EQUAL,
                                new SearchedCaseExpression(
                                        ImmutableList.of(
                                                new WhenClause(aEqualsTo(1), ONE),
                                                new WhenClause(aEqualsTo(2), ONE),
                                                new WhenClause(aEqualsTo(3), ONE)),
                                        Optional.of(ZERO)),
                                ONE),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(new LogicalExpression(OR, ImmutableList.of(aEqualsTo(1L), aEqualsTo(2L), aEqualsTo(3L))),
                        values("a")));

        // CASE WHEN a = 1 THEN 1 WHEN a = 2 THEN 2 WHEN a = 3 WHEN 1 ELSE 1 END = 1
        tester()
                .assertThat(new RemoveRedundantCaseWhenLiteral(tester().getPlannerContext(), tester().getTypeAnalyzer()))
                .on(p -> p.filter(
                        new ComparisonExpression(
                                EQUAL,
                                new SearchedCaseExpression(
                                        ImmutableList.of(
                                                new WhenClause(aEqualsTo(1), ONE),
                                                new WhenClause(aEqualsTo(2), ZERO),
                                                new WhenClause(aEqualsTo(3), ONE)),
                                        Optional.of(ONE)),
                                ONE),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new LogicalExpression(OR, ImmutableList.of(aEqualsTo(1), aEqualsTo(3), new NotExpression(aEqualsTo(2)))),
                                values("a")));
    }

    @Test
    public void testString()
    {
        // CASE WHEN a = 'xyz' THEN 'aa' ELSE 'bbb' END = 'aa'
        tester()
                .assertThat(new RemoveRedundantCaseWhenLiteral(tester().getPlannerContext(), tester().getTypeAnalyzer()))
                .on(p -> p.filter(
                        new ComparisonExpression(
                                EQUAL,
                                new SearchedCaseExpression(
                                        ImmutableList.of(
                                                new WhenClause(
                                                        new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(VARCHAR, Slices.utf8Slice("xyz"))),
                                                        new Constant(VARCHAR, Slices.utf8Slice("aa")))),
                                        Optional.of(new Constant(VARCHAR, Slices.utf8Slice("bbb")))),
                                new Constant(VARCHAR, Slices.utf8Slice("aa"))),
                        p.values(p.symbol("a", VARCHAR))))
                .matches(
                        filter(
                                new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(VARCHAR, Slices.utf8Slice("xyz"))),
                                values("a")));
    }

    @Test
    public void testDate()
    {
        // CASE WHEN CAST(a as DATE) = date '2023-11-01' THEN date '2023-01-01' ELSE date '2023-02-02' END = date '2023-01-01'
        tester()
                .assertThat(new RemoveRedundantCaseWhenLiteral(tester().getPlannerContext(), tester().getTypeAnalyzer()))
                .on(p -> p.filter(
                        new ComparisonExpression(
                                EQUAL,
                                new SearchedCaseExpression(
                                        ImmutableList.of(
                                                new WhenClause(new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), DATE), date("2024-2-1")), date("2024-1-1"))),
                                        Optional.of(date("2023-1-1"))),
                                date("2024-1-1")),
                        p.values(p.symbol("a", VARCHAR))))
                .matches(
                        filter(
                                new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), DATE), date("2024-2-1")),
                                values("a")));
    }

    private static ComparisonExpression aEqualsTo(long value)
    {
        return new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(INTEGER, value));
    }

    private static Constant date(String ds)
    {
        return new Constant(DATE, (long) DateTimeUtils.parseDate(ds));
    }
}
