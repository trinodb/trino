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
package io.trino.sql.planner.planprinter;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCounterBasedAnonymizer
{
    @Test
    public void testTimestampWithTimeZoneValueAnonymization()
    {
        CounterBasedAnonymizer anonymizer = new CounterBasedAnonymizer();
        assertThat(anonymizer.anonymize(TIMESTAMP_TZ_MILLIS, "2012-10-30 18:00:00.000 America/Los_Angeles"))
                .isEqualTo("timestamp_3_with_time_zone_value_1");
    }

    @Test
    public void testSymbolReferenceAnonymization()
    {
        LogicalExpression expression = new LogicalExpression(AND, ImmutableList.of(
                new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("1")),
                new ComparisonExpression(LESS_THAN, new SymbolReference("b"), new LongLiteral("2")),
                new ComparisonExpression(EQUAL, new SymbolReference("c"), new LongLiteral("3"))));
        CounterBasedAnonymizer anonymizer = new CounterBasedAnonymizer();
        assertThat(anonymizer.anonymize(expression))
                .isEqualTo("((\"symbol_1\" > 'long_literal_1') AND (\"symbol_2\" < 'long_literal_2') AND (\"symbol_3\" = 'long_literal_3'))");
    }

    @Test(dataProvider = "literals")
    public void testLiteralAnonymization(Literal actual, String expected)
    {
        CounterBasedAnonymizer anonymizer = new CounterBasedAnonymizer();
        assertThat(anonymizer.anonymize(actual)).isEqualTo(expected);
    }

    @DataProvider
    public static Object[][] literals()
    {
        return new Object[][] {
                {new BinaryLiteral("DEF321"), "'binary_literal_1'"},
                {new StringLiteral("abc"), "'string_literal_1'"},
                {new GenericLiteral("bigint", "1"), "'bigint_literal_1'"},
                {new CharLiteral("a"), "'char_literal_1'"},
                {new DecimalLiteral("123"), "'decimal_literal_1'"},
                {new DoubleLiteral(String.valueOf(6554)), "'double_literal_1'"},
                {new DoubleLiteral(String.valueOf(Double.MAX_VALUE)), "'double_literal_1'"},
                {new LongLiteral(String.valueOf(6554)), "'long_literal_1'"},
                {new LongLiteral(String.valueOf(Long.MAX_VALUE)), "'long_literal_1'"},
                {new BooleanLiteral("true"), "true"},
                {new TimeLiteral("03:04:05"), "'time_literal_1'"},
                {new TimestampLiteral("2012-10-31 01:00 UTC"), "'timestamp_literal_1'"},
                {new NullLiteral(), "null"},
                {
                    new IntervalLiteral("33", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.DAY, Optional.empty()),
                    "'interval_literal_1'"
                }
        };
    }
}
