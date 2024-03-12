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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.LongTimestamp;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.UnwrapYearInComparison.calculateRangeEndInclusive;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.sql.tree.LogicalExpression.Operator.OR;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUnwrapYearInComparison
        extends BasePlanTest
{
    @Test
    public void testEquals()
    {
        testUnwrap("date", "year(a) = -0001", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "-0001-01-01"), new GenericLiteral("DATE", "-0001-12-31")));
        testUnwrap("date", "year(a) = 1960", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "1960-01-01"), new GenericLiteral("DATE", "1960-12-31")));
        testUnwrap("date", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "2022-01-01"), new GenericLiteral("DATE", "2022-12-31")));
        testUnwrap("date", "year(a) = 9999", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "9999-01-01"), new GenericLiteral("DATE", "9999-12-31")));

        testUnwrap("timestamp", "year(a) = -0001", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "-0001-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "-0001-12-31 23:59:59.999")));
        testUnwrap("timestamp", "year(a) = 1960", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1960-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "1960-12-31 23:59:59.999")));
        testUnwrap("timestamp", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999")));
        testUnwrap("timestamp", "year(a) = 9999", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "9999-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "9999-12-31 23:59:59.999")));

        testUnwrap("timestamp(0)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59")));
        testUnwrap("timestamp(1)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9")));
        testUnwrap("timestamp(2)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99")));
        testUnwrap("timestamp(3)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999")));
        testUnwrap("timestamp(4)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999")));
        testUnwrap("timestamp(5)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999")));
        testUnwrap("timestamp(6)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999")));
        testUnwrap("timestamp(7)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999999")));
        testUnwrap("timestamp(8)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999999")));
        testUnwrap("timestamp(9)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999999")));
        testUnwrap("timestamp(10)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999999999")));
        testUnwrap("timestamp(11)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999999999")));
        testUnwrap("timestamp(12)", "year(a) = 2022", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999999999")));
    }

    @Test
    public void testInPredicate()
    {
        testUnwrap("date", "year(a) IN (1000, 1400, 1800)", new LogicalExpression(OR, ImmutableList.of(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "1000-01-01"), new GenericLiteral("DATE", "1000-12-31")), new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "1400-01-01"), new GenericLiteral("DATE", "1400-12-31")), new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "1800-01-01"), new GenericLiteral("DATE", "1800-12-31")))));
        testUnwrap("timestamp", "year(a) IN (1000, 1400, 1800)", new LogicalExpression(OR, ImmutableList.of(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1000-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "1000-12-31 23:59:59.999")), new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1400-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "1400-12-31 23:59:59.999")), new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1800-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "1800-12-31 23:59:59.999")))));
    }

    @Test
    public void testNotEquals()
    {
        testUnwrap("date", "year(a) <> -0001", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "-0001-01-01"), new GenericLiteral("DATE", "-0001-12-31"))));
        testUnwrap("date", "year(a) <> 1960", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "1960-01-01"), new GenericLiteral("DATE", "1960-12-31"))));
        testUnwrap("date", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "2022-01-01"), new GenericLiteral("DATE", "2022-12-31"))));
        testUnwrap("date", "year(a) <> 9999", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "9999-01-01"), new GenericLiteral("DATE", "9999-12-31"))));

        testUnwrap("timestamp", "year(a) <> -0001", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "-0001-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "-0001-12-31 23:59:59.999"))));
        testUnwrap("timestamp", "year(a) <> 1960", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1960-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "1960-12-31 23:59:59.999"))));
        testUnwrap("timestamp", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999"))));
        testUnwrap("timestamp", "year(a) <> 9999", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "9999-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "9999-12-31 23:59:59.999"))));

        testUnwrap("timestamp(0)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59"))));
        testUnwrap("timestamp(1)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9"))));
        testUnwrap("timestamp(2)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99"))));
        testUnwrap("timestamp(3)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999"))));
        testUnwrap("timestamp(4)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999"))));
        testUnwrap("timestamp(5)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999"))));
        testUnwrap("timestamp(6)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999"))));
        testUnwrap("timestamp(7)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999999"))));
        testUnwrap("timestamp(8)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999999"))));
        testUnwrap("timestamp(9)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999999"))));
        testUnwrap("timestamp(10)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999999999"))));
        testUnwrap("timestamp(11)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999999999"))));
        testUnwrap("timestamp(12)", "year(a) <> 2022", new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999999999"))));
    }

    @Test
    public void testLessThan()
    {
        testUnwrap("date", "year(a) < -0001", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "-0001-01-01")));
        testUnwrap("date", "year(a) < 1960", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "1960-01-01")));
        testUnwrap("date", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2022-01-01")));
        testUnwrap("date", "year(a) < 9999", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "9999-01-01")));

        testUnwrap("timestamp", "year(a) < -0001", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "-0001-01-01 00:00:00.000")));
        testUnwrap("timestamp", "year(a) < 1960", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1960-01-01 00:00:00.000")));
        testUnwrap("timestamp", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000")));
        testUnwrap("timestamp", "year(a) < 9999", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "9999-01-01 00:00:00.000")));

        testUnwrap("timestamp(0)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00")));
        testUnwrap("timestamp(1)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0")));
        testUnwrap("timestamp(2)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00")));
        testUnwrap("timestamp(3)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000")));
        testUnwrap("timestamp(4)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000")));
        testUnwrap("timestamp(5)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000")));
        testUnwrap("timestamp(6)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000")));
        testUnwrap("timestamp(7)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000000")));
        testUnwrap("timestamp(8)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000000")));
        testUnwrap("timestamp(9)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000000")));
        testUnwrap("timestamp(10)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000000000")));
        testUnwrap("timestamp(11)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000000000")));
        testUnwrap("timestamp(12)", "year(a) < 2022", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000000000")));
    }

    @Test
    public void testLessThanOrEqual()
    {
        testUnwrap("date", "year(a) <= -0001", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "-0001-12-31")));
        testUnwrap("date", "year(a) <= 1960", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1960-12-31")));
        testUnwrap("date", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "2022-12-31")));
        testUnwrap("date", "year(a) <= 9999", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "9999-12-31")));

        testUnwrap("timestamp", "year(a) <= -0001", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "-0001-12-31 23:59:59.999")));
        testUnwrap("timestamp", "year(a) <= 1960", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1960-12-31 23:59:59.999")));
        testUnwrap("timestamp", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999")));
        testUnwrap("timestamp", "year(a) <= 9999", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "9999-12-31 23:59:59.999")));

        testUnwrap("timestamp(0)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59")));
        testUnwrap("timestamp(1)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9")));
        testUnwrap("timestamp(2)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99")));
        testUnwrap("timestamp(3)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999")));
        testUnwrap("timestamp(4)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999")));
        testUnwrap("timestamp(5)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999")));
        testUnwrap("timestamp(6)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999")));
        testUnwrap("timestamp(7)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999999")));
        testUnwrap("timestamp(8)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999999")));
        testUnwrap("timestamp(9)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999999")));
        testUnwrap("timestamp(10)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999999999")));
        testUnwrap("timestamp(11)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999999999")));
        testUnwrap("timestamp(12)", "year(a) <= 2022", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999999999")));
    }

    @Test
    public void testGreaterThan()
    {
        testUnwrap("date", "year(a) > -0001", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "-0001-12-31")));
        testUnwrap("date", "year(a) > 1960", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "1960-12-31")));
        testUnwrap("date", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2022-12-31")));
        testUnwrap("date", "year(a) > 9999", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "9999-12-31")));

        testUnwrap("timestamp", "year(a) > -0001", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "-0001-12-31 23:59:59.999")));
        testUnwrap("timestamp", "year(a) > 1960", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1960-12-31 23:59:59.999")));
        testUnwrap("timestamp", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999")));
        testUnwrap("timestamp", "year(a) > 9999", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "9999-12-31 23:59:59.999")));

        testUnwrap("timestamp(0)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59")));
        testUnwrap("timestamp(1)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9")));
        testUnwrap("timestamp(2)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99")));
        testUnwrap("timestamp(3)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999")));
        testUnwrap("timestamp(4)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999")));
        testUnwrap("timestamp(5)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999")));
        testUnwrap("timestamp(6)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999")));
        testUnwrap("timestamp(7)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999999")));
        testUnwrap("timestamp(8)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999999")));
        testUnwrap("timestamp(9)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999999")));
        testUnwrap("timestamp(10)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999999999")));
        testUnwrap("timestamp(11)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999999999")));
        testUnwrap("timestamp(12)", "year(a) > 2022", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999999999")));
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        testUnwrap("date", "year(a) >= -0001", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "-0001-01-01")));
        testUnwrap("date", "year(a) >= 1960", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1960-01-01")));
        testUnwrap("date", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "2022-01-01")));
        testUnwrap("date", "year(a) >= 9999", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "9999-01-01")));

        testUnwrap("timestamp", "year(a) >= -0001", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "-0001-01-01 00:00:00.000")));
        testUnwrap("timestamp", "year(a) >= 1960", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1960-01-01 00:00:00.000")));
        testUnwrap("timestamp", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000")));
        testUnwrap("timestamp", "year(a) >= 9999", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "9999-01-01 00:00:00.000")));

        testUnwrap("timestamp(0)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00")));
        testUnwrap("timestamp(1)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0")));
        testUnwrap("timestamp(2)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00")));
        testUnwrap("timestamp(3)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000")));
        testUnwrap("timestamp(4)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000")));
        testUnwrap("timestamp(5)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000")));
        testUnwrap("timestamp(6)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000")));
        testUnwrap("timestamp(7)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000000")));
        testUnwrap("timestamp(8)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000000")));
        testUnwrap("timestamp(9)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000000")));
        testUnwrap("timestamp(10)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000000000")));
        testUnwrap("timestamp(11)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000000000")));
        testUnwrap("timestamp(12)", "year(a) >= 2022", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000000000")));
    }

    @Test
    public void testDistinctFrom()
    {
        testUnwrap("date", "year(a) IS DISTINCT FROM -0001", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "-0001-01-01"), new GenericLiteral("DATE", "-0001-12-31"))))));
        testUnwrap("date", "year(a) IS DISTINCT FROM 1960", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "1960-01-01"), new GenericLiteral("DATE", "1960-12-31"))))));
        testUnwrap("date", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "2022-01-01"), new GenericLiteral("DATE", "2022-12-31"))))));
        testUnwrap("date", "year(a) IS DISTINCT FROM 9999", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "9999-01-01"), new GenericLiteral("DATE", "9999-12-31"))))));

        testUnwrap("timestamp", "year(a) IS DISTINCT FROM -0001", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "-0001-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "-0001-12-31 23:59:59.999"))))));
        testUnwrap("timestamp", "year(a) IS DISTINCT FROM 1960", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1960-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "1960-12-31 23:59:59.999"))))));
        testUnwrap("timestamp", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999"))))));
        testUnwrap("timestamp", "year(a) IS DISTINCT FROM 9999", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "9999-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "9999-12-31 23:59:59.999"))))));

        testUnwrap("timestamp(0)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59"))))));
        testUnwrap("timestamp(1)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9"))))));
        testUnwrap("timestamp(2)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99"))))));
        testUnwrap("timestamp(3)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999"))))));
        testUnwrap("timestamp(4)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999"))))));
        testUnwrap("timestamp(5)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999"))))));
        testUnwrap("timestamp(6)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999"))))));
        testUnwrap("timestamp(7)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999999"))))));
        testUnwrap("timestamp(8)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999999"))))));
        testUnwrap("timestamp(9)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999999"))))));
        testUnwrap("timestamp(10)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.0000000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.9999999999"))))));
        testUnwrap("timestamp(11)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.00000000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.99999999999"))))));
        testUnwrap("timestamp(12)", "year(a) IS DISTINCT FROM 2022", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2022-01-01 00:00:00.000000000000"), new GenericLiteral("TIMESTAMP", "2022-12-31 23:59:59.999999999999"))))));
    }

    @Test
    public void testNull()
    {
        testUnwrap("date", "year(a) = CAST(NULL AS BIGINT)", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("timestamp", "year(a) = CAST(NULL AS BIGINT)", new Cast(new NullLiteral(), dataType("boolean")));
    }

    @Test
    public void testNaN()
    {
        testUnwrap("date", "year(a) = nan()", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new FunctionCall(QualifiedName.of("year"), ImmutableList.of(new SymbolReference("a")))), new Cast(new NullLiteral(), dataType("boolean")))));
        testUnwrap("timestamp", "year(a) = nan()", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new FunctionCall(QualifiedName.of("year"), ImmutableList.of(new SymbolReference("a")))), new Cast(new NullLiteral(), dataType("boolean")))));
    }

    @Test
    public void smokeTests()
    {
        // smoke tests for various type combinations
        for (String type : asList("SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            testUnwrap("date", format("year(a) = %s '2022'", type), new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "2022-01-01"), new GenericLiteral("DATE", "2022-12-31")));
        }
    }

    @Test
    public void testTermOrder()
    {
        // ensure the optimization works when the terms of the comparison are reversed
        // vs the canonical <expr> <op> <literal> form
        assertPlan("SELECT * FROM (VALUES DATE '2022-01-01') t(a) WHERE 2022 = year(a)",
                output(
                        filter(
                                new BetweenPredicate(new SymbolReference("A"), new GenericLiteral("DATE", "2022-01-01"), new GenericLiteral("DATE", "2022-12-31")),
                                values("A"))));
    }

    @Test
    public void testLeapYear()
    {
        testUnwrap("date", "year(a) = 2024", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("DATE", "2024-01-01"), new GenericLiteral("DATE", "2024-12-31")));
        testUnwrap("timestamp", "year(a) = 2024", new BetweenPredicate(new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2024-01-01 00:00:00.000"), new GenericLiteral("TIMESTAMP", "2024-12-31 23:59:59.999")));
    }

    @Test
    public void testCalculateRangeEndInclusive()
    {
        assertThat(calculateRangeEndInclusive(1960, DATE)).isEqualTo(LocalDate.of(1960, 12, 31).toEpochDay());
        assertThat(calculateRangeEndInclusive(2024, DATE)).isEqualTo(LocalDate.of(2024, 12, 31).toEpochDay());

        assertThat(calculateRangeEndInclusive(1960, TIMESTAMP_SECONDS)).isEqualTo(toEpochMicros(LocalDateTime.of(1960, 12, 31, 23, 59, 59)));
        assertThat(calculateRangeEndInclusive(1960, TIMESTAMP_MILLIS)).isEqualTo(toEpochMicros(LocalDateTime.of(1960, 12, 31, 23, 59, 59, 999_000_000)));
        assertThat(calculateRangeEndInclusive(1960, TIMESTAMP_MICROS)).isEqualTo(toEpochMicros(LocalDateTime.of(1960, 12, 31, 23, 59, 59, 999_999_000)));
        assertThat(calculateRangeEndInclusive(1960, TIMESTAMP_NANOS)).isEqualTo(new LongTimestamp(toEpochMicros(LocalDateTime.of(1960, 12, 31, 23, 59, 59, 999_999_000)), 999_000));
        assertThat(calculateRangeEndInclusive(1960, TIMESTAMP_PICOS)).isEqualTo(new LongTimestamp(toEpochMicros(LocalDateTime.of(1960, 12, 31, 23, 59, 59, 999_999_000)), 999_999));
        assertThat(calculateRangeEndInclusive(2024, TIMESTAMP_SECONDS)).isEqualTo(toEpochMicros(LocalDateTime.of(2024, 12, 31, 23, 59, 59)));
        assertThat(calculateRangeEndInclusive(2024, TIMESTAMP_MILLIS)).isEqualTo(toEpochMicros(LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999_000_000)));
        assertThat(calculateRangeEndInclusive(2024, TIMESTAMP_MICROS)).isEqualTo(toEpochMicros(LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999_999_000)));
        assertThat(calculateRangeEndInclusive(2024, TIMESTAMP_NANOS)).isEqualTo(new LongTimestamp(toEpochMicros(LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999_999_000)), 999_000));
        assertThat(calculateRangeEndInclusive(2024, TIMESTAMP_PICOS)).isEqualTo(new LongTimestamp(toEpochMicros(LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999_999_000)), 999_999));
    }

    private static long toEpochMicros(LocalDateTime localDateTime)
    {
        return multiplyExact(localDateTime.toEpochSecond(UTC), MICROSECONDS_PER_SECOND)
                + localDateTime.getNano() / NANOSECONDS_PER_MICROSECOND;
    }

    private void testUnwrap(String inputType, String inputPredicate, Expression expected)
    {
        Expression antiOptimization = new ComparisonExpression(EQUAL, new FunctionCall(QualifiedName.of("random"), ImmutableList.of()), new DoubleLiteral("42.0"));
        if (expected instanceof LogicalExpression logical && logical.getOperator() == OR) {
            expected = new LogicalExpression(OR, ImmutableList.<Expression>builder()
                    .addAll(logical.getTerms())
                    .add(antiOptimization)
                    .build());
        }
        else {
            expected = new LogicalExpression(OR, ImmutableList.of(expected, antiOptimization));
        }

        String sql = format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE %s OR rand() = 42", inputType, inputPredicate);
        try {
            assertPlan(
                    sql,
                    getPlanTester().getDefaultSession(),
                    output(
                            filter(
                                    expected,
                                    values("a"))));
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("Query: " + sql));
            throw e;
        }
    }
}
