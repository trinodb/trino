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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.operator.scalar.BitwiseFunctions;
import io.trino.operator.scalar.JoniRegexpFunctions;
import io.trino.operator.scalar.JsonFunctions;
import io.trino.operator.scalar.JsonPath;
import io.trino.operator.scalar.MathFunctions;
import io.trino.operator.scalar.StringFunctions;
import io.trino.operator.scalar.timestamp.ExtractDay;
import io.trino.operator.scalar.timestamp.ExtractDayOfWeek;
import io.trino.operator.scalar.timestamp.ExtractDayOfYear;
import io.trino.operator.scalar.timestamp.ExtractHour;
import io.trino.operator.scalar.timestamp.ExtractMinute;
import io.trino.operator.scalar.timestamp.ExtractMonth;
import io.trino.operator.scalar.timestamp.ExtractQuarter;
import io.trino.operator.scalar.timestamp.ExtractSecond;
import io.trino.operator.scalar.timestamp.ExtractWeekOfYear;
import io.trino.operator.scalar.timestamp.ExtractYear;
import io.trino.operator.scalar.timestamp.ExtractYearOfWeek;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.query.QueryAssertions;
import io.trino.sql.tree.Extract.Field;
import io.trino.type.LikeFunctions;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.stream.LongStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.scalar.JoniRegexpCasts.joniRegexp;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.tree.Extract.Field.TIMEZONE_HOUR;
import static io.trino.sql.tree.Extract.Field.TIMEZONE_MINUTE;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.type.DateTimes.MICROSECONDS_PER_MILLISECOND;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.lang.Math.cos;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestExpressionCompiler
{
    private static final Boolean[] booleanValues = {true, false, null};
    private static final Integer[] smallInts = {9, 10, 11, -9, -10, -11, null};
    private static final Integer[] extremeInts = {101510, /*Long.MIN_VALUE,*/ Integer.MAX_VALUE};
    private static final Integer[] intLefts = ObjectArrays.concat(smallInts, extremeInts, Integer.class);
    private static final Integer[] intRights = {3, -3, 101510823, null};
    private static final Integer[] intMiddle = {9, -3, 88, null};
    private static final Double[] doubleLefts = {9.0, 10.0, 11.0, -9.0, -10.0, -11.0, 9.1, 10.1, 11.1, -9.1, -10.1, -11.1,
            Double.MIN_VALUE, Double.MAX_VALUE, Double.MIN_NORMAL, null};
    private static final Double[] doubleRights = {3.0, -3.0, 3.1, -3.1, null};
    private static final Double[] doubleMiddle = {9.0, -3.1, 88.0, null};
    private static final String[] stringLefts = {"hello", "foo", "mellow", "fellow", "", null};
    private static final String[] stringRights = {"hello", "foo", "bar", "baz", "", null};
    private static final Long[] longLefts = {9L, 10L, 11L, -9L, -10L, -11L, null};
    private static final Long[] longRights = {3L, -3L, 10151082135029369L, null};
    private static final BigDecimal[] decimalLefts = {new BigDecimal("9.0"), new BigDecimal("10.0"), new BigDecimal("11.0"), new BigDecimal("-9.0"),
            new BigDecimal("-10.0"), new BigDecimal("-11.0"), new BigDecimal("9.1"), new BigDecimal("10.1"),
            new BigDecimal("11.1"), new BigDecimal("-9.1"), new BigDecimal("-10.1"), new BigDecimal("-11.1"),
            new BigDecimal("9223372036.5477"), new BigDecimal("-9223372036.5477"), null};
    private static final BigDecimal[] decimalRights = {new BigDecimal("3.0"), new BigDecimal("-3.0"), new BigDecimal("3.1"), new BigDecimal("-3.1"), null};
    private static final BigDecimal[] decimalMiddle = {new BigDecimal("9.0"), new BigDecimal("-3.1"), new BigDecimal("88.0"), null};

    private QueryAssertions assertions;

    @BeforeAll
    public void setup()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void tearDown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testUnaryOperators()
    {
        assertThat(assertions.expression("a IS NULL")
                .binding("a", "CAST(null AS boolean)"))
                .isEqualTo(true);

        for (Boolean value : booleanValues) {
            assertThat(assertions.expression(toLiteral(value)))
                    .hasType(BOOLEAN)
                    .isEqualTo(value == null ? null : value);

            assertThat(assertions.expression("a IS NULL")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null);

            assertThat(assertions.expression("a IS NOT NULL")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value != null);
        }

        for (Integer value : intLefts) {
            Long longValue = value == null ? null : value * 10000000000L;

            assertThat(assertions.expression(toLiteral(value)))
                    .hasType(INTEGER)
                    .isEqualTo(value == null ? null : value);

            assertThat(assertions.expression("- a")
                    .binding("a", toLiteral(value)))
                    .hasType(INTEGER)
                    .isEqualTo(value == null ? null : -value);

            assertThat(assertions.expression(toLiteral(longValue)))
                    .hasType(BIGINT)
                    .isEqualTo(value == null ? null : longValue);

            assertThat(assertions.expression("- a")
                    .binding("a", toLiteral(longValue)))
                    .isEqualTo(value == null ? null : -longValue);

            assertThat(assertions.expression("a IS NULL")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null);

            assertThat(assertions.expression("a IS NOT NULL")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value != null);
        }

        for (Double value : doubleLefts) {
            assertThat(assertions.expression(toLiteral(value)))
                    .hasType(DOUBLE)
                    .isEqualTo(value == null ? null : value);

            assertThat(assertions.expression("- a")
                    .binding("a", toLiteral(value)))
                    .hasType(DOUBLE)
                    .isEqualTo(value == null ? null : -value);

            assertThat(assertions.expression("a IS NULL")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null);

            assertThat(assertions.expression("a IS NOT NULL")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value != null);
        }

        for (BigDecimal value : decimalLefts) {
            assertThat(assertions.expression(toLiteral(value)))
                    .isEqualTo(value == null ? null : toSqlDecimal(value));

            assertThat(assertions.expression("- a")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : toSqlDecimal(value.negate()));

            assertThat(assertions.expression("a IS NULL")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null);

            assertThat(assertions.expression("a IS NOT NULL")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value != null);
        }

        for (String value : stringLefts) {
            assertThat(assertions.expression(toLiteral(value)))
                    .hasType(varcharType(value))
                    .isEqualTo(value == null ? null : value);

            assertThat(assertions.expression("a IS NULL")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null);

            assertThat(assertions.expression("a IS NOT NULL")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value != null);
        }
    }

    @Test
    public void testBinaryOperatorsBoolean()
    {
        assertThat(assertions.expression("nullif(a, true)")
                .binding("a", "CAST(null AS boolean)"))
                .isNull(BOOLEAN);

        for (Boolean left : booleanValues) {
            for (Boolean right : booleanValues) {
                assertThat(assertions.expression("a = b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.equals(right));

                assertThat(assertions.expression("a <> b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : !left.equals(right));

                assertThat(assertions.expression("nullif(a, b)")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(nullIf(left, right));

                assertThat(assertions.expression("a IS DISTINCT FROM b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(!Objects.equals(left, right));
            }
        }
    }

    @Test
    public void testBinaryOperatorsIntegralIntegral()
    {
        for (Integer left : smallInts) {
            for (Integer right : intRights) {
                assertThat(assertions.expression("a = b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (long) left == right);

                assertThat(assertions.expression("a <> b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (long) left != right);

                assertThat(assertions.expression("a > b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (long) left > right);

                assertThat(assertions.expression("a < b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (long) left < right);

                assertThat(assertions.expression("a >= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (long) left >= right);

                assertThat(assertions.expression("a <= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (long) left <= right);

                assertThat(assertions.function("nullif", toLiteral(left), toLiteral(right)))
                        .hasType(INTEGER)
                        .isEqualTo(nullIf(left, right));

                assertThat(assertions.expression("a IS DISTINCT FROM b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(!Objects.equals(left, right));

                assertThat(assertions.expression("a + b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(INTEGER)
                        .isEqualTo(left == null || right == null ? null : left + right);

                assertThat(assertions.expression("a - b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(INTEGER)
                        .isEqualTo(left == null || right == null ? null : left - right);

                assertThat(assertions.expression("a * b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(INTEGER)
                        .isEqualTo(left == null || right == null ? null : left * right);

                assertThat(assertions.expression("a / b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(INTEGER)
                        .isEqualTo(left == null || right == null ? null : left / right);

                assertThat(assertions.expression("a % b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(INTEGER)
                        .isEqualTo(left == null || right == null ? null : left % right);

                Long longLeft = left == null ? null : left * 1000000000L;
                assertThat(assertions.expression("a + b")
                        .binding("a", toLiteral(longLeft))
                        .binding("b", toLiteral(right)))
                        .hasType(BIGINT)
                        .isEqualTo(longLeft == null || right == null ? null : longLeft + right);
                assertThat(assertions.expression("a - b")
                        .binding("a", toLiteral(longLeft))
                        .binding("b", toLiteral(right)))
                        .hasType(BIGINT)
                        .isEqualTo(longLeft == null || right == null ? null : longLeft - right);
                assertThat(assertions.expression("a * b")
                        .binding("a", toLiteral(longLeft))
                        .binding("b", toLiteral(right)))
                        .hasType(BIGINT)
                        .isEqualTo(longLeft == null || right == null ? null : longLeft * right);
                assertThat(assertions.expression("a / b")
                        .binding("a", toLiteral(longLeft))
                        .binding("b", toLiteral(right)))
                        .hasType(BIGINT)
                        .isEqualTo(longLeft == null || right == null ? null : longLeft / right);
                assertThat(assertions.expression("a % b")
                        .binding("a", toLiteral(longLeft))
                        .binding("b", toLiteral(right)))
                        .hasType(BIGINT)
                        .isEqualTo(longLeft == null || right == null ? null : longLeft % right);
            }
        }
    }

    @Test
    public void testBinaryOperatorsIntegralDouble()
    {
        for (Integer left : intLefts) {
            for (Double right : doubleRights) {
                assertThat(assertions.expression("a = b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (double) left == right);

                assertThat(assertions.expression("a <> b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (double) left != right);

                assertThat(assertions.expression("a > b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (double) left > right);

                assertThat(assertions.expression("a < b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (double) left < right);

                assertThat(assertions.expression("a >= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (double) left >= right);

                assertThat(assertions.expression("a <= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (double) left <= right);

                assertThat(assertions.expression("nullif(a, b)")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(INTEGER)
                        .isEqualTo(nullIf(left, right));

                assertThat(assertions.expression("a IS DISTINCT FROM b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(!Objects.equals(left == null ? null : left.doubleValue(), right));

                assertThat(assertions.expression("a + b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left + right);

                assertThat(assertions.expression("a - b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left - right);

                assertThat(assertions.expression("a * b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left * right);

                assertThat(assertions.expression("a / b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left / right);

                assertThat(assertions.expression("a % b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left % right);
            }
        }
    }

    @Test
    public void testBinaryOperatorsDoubleIntegral()
    {
        for (Double left : doubleLefts) {
            for (Integer right : intRights) {
                assertThat(assertions.expression("a = b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left == (double) right);

                assertThat(assertions.expression("a <> b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left != (double) right);

                assertThat(assertions.expression("a > b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left > (double) right);

                assertThat(assertions.expression("a < b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left < (double) right);

                assertThat(assertions.expression("a >= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left >= (double) right);

                assertThat(assertions.expression("a <= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left <= (double) right);

                assertThat(assertions.function("nullif", toLiteral(left), toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(nullIf(left, right));

                assertThat(assertions.expression("a IS DISTINCT FROM b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .isEqualTo(!Objects.equals(left, right == null ? null : right.doubleValue()));

                assertThat(assertions.expression("a + b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left + right);

                assertThat(assertions.expression("a - b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left - right);

                assertThat(assertions.expression("a * b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left * right);

                assertThat(assertions.expression("a / b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left / right);

                assertThat(assertions.expression("a % b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left % right);
            }
        }
    }

    @Test
    public void testBinaryOperatorsDoubleDouble()
    {
        for (Double left : doubleLefts) {
            for (Double right : doubleRights) {
                assertThat(assertions.expression("a = b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (double) left == right);

                assertThat(assertions.expression("a <> b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (double) left != right);

                assertThat(assertions.expression("a > b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (double) left > right);

                assertThat(assertions.expression("a < b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (double) left < right);

                assertThat(assertions.expression("a >= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (double) left >= right);

                assertThat(assertions.expression("a <= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : (double) left <= right);

                assertThat(assertions.function("nullif", toLiteral(left), toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(nullIf(left, right));

                assertThat(assertions.expression("a IS DISTINCT FROM b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(!Objects.equals(left, right));

                assertThat(assertions.expression("a + b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left + right);

                assertThat(assertions.expression("a - b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left - right);

                assertThat(assertions.expression("a * b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left * right);
                assertThat(assertions.expression("a / b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left / right);
                assertThat(assertions.expression("a % b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left % right);
            }
        }
    }

    @Test
    public void testBinaryOperatorsDecimalBigint()
    {
        for (BigDecimal left : decimalLefts) {
            for (Long right : longRights) {
                assertThat(assertions.expression("a = b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.equals(new BigDecimal(right)));

                assertThat(assertions.expression("a <> b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : !left.equals(new BigDecimal(right)));

                assertThat(assertions.expression("a > b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.compareTo(new BigDecimal(right)) > 0);

                assertThat(assertions.expression("a < b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.compareTo(new BigDecimal(right)) < 0);

                assertThat(assertions.expression("a >= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.compareTo(new BigDecimal(right)) >= 0);

                assertThat(assertions.expression("a <= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.compareTo(new BigDecimal(right)) <= 0);

                assertThat(assertions.function("nullif", toLiteral(left), toLiteral(right)))
                        .isEqualTo(toSqlDecimal(BigDecimal.class.cast(nullIf(left, right))));

                assertThat(assertions.expression("a IS DISTINCT FROM b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(!Objects.equals(left, right == null ? null : new BigDecimal(right)));

                // arithmetic operators are already tested in TestDecimalOperators
            }
        }
    }

    @Test
    public void testBinaryOperatorsBigintDecimal()
    {
        for (Long left : longLefts) {
            for (BigDecimal right : decimalRights) {
                assertThat(assertions.expression("a = b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : new BigDecimal(left).equals(right));

                assertThat(assertions.expression("a <> b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : !new BigDecimal(left).equals(right));

                assertThat(assertions.expression("a > b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : new BigDecimal(left).compareTo(right) > 0);

                assertThat(assertions.expression("a < b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : new BigDecimal(left).compareTo(right) < 0);

                assertThat(assertions.expression("a >= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : new BigDecimal(left).compareTo(right) >= 0);

                assertThat(assertions.expression("a <= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : new BigDecimal(left).compareTo(right) <= 0);

                assertThat(assertions.function("nullif", toLiteral(left), toLiteral(right)))
                        .hasType(BIGINT)
                        .isEqualTo(left);

                assertThat(assertions.expression("a IS DISTINCT FROM b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(!Objects.equals(left == null ? null : new BigDecimal(left), right));

                // arithmetic operators are already tested in TestDecimalOperators
            }
        }
    }

    @Test
    public void testBinaryOperatorsDecimalInteger()
    {
        for (BigDecimal left : decimalLefts) {
            for (Integer right : intRights) {
                assertThat(assertions.expression("a = b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.equals(new BigDecimal(right)));

                assertThat(assertions.expression("a <> b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : !left.equals(new BigDecimal(right)));

                assertThat(assertions.expression("a > b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.compareTo(new BigDecimal(right)) > 0);

                assertThat(assertions.expression("a < b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.compareTo(new BigDecimal(right)) < 0);

                assertThat(assertions.expression("a >= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.compareTo(new BigDecimal(right)) >= 0);

                assertThat(assertions.expression("a <= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.compareTo(new BigDecimal(right)) <= 0);

                assertThat(assertions.function("nullif", toLiteral(left), toLiteral(right)))
                        .isEqualTo(toSqlDecimal(BigDecimal.class.cast(nullIf(left, right))));

                assertThat(assertions.expression("a IS DISTINCT FROM b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(!Objects.equals(left, right == null ? null : new BigDecimal(right)));

                // arithmetic operators are already tested in TestDecimalOperators
            }
        }
    }

    @Test
    public void testBinaryOperatorsIntegerDecimal()
    {
        for (Integer left : intLefts) {
            for (BigDecimal right : decimalRights) {
                assertThat(assertions.expression("a = b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : new BigDecimal(left).equals(right));

                assertThat(assertions.expression("a <> b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : !new BigDecimal(left).equals(right));

                assertThat(assertions.expression("a > b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : new BigDecimal(left).compareTo(right) > 0);

                assertThat(assertions.expression("a < b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : new BigDecimal(left).compareTo(right) < 0);

                assertThat(assertions.expression("a >= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : new BigDecimal(left).compareTo(right) >= 0);

                assertThat(assertions.expression("a <= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : new BigDecimal(left).compareTo(right) <= 0);

                assertThat(assertions.function("nullif", toLiteral(left), toLiteral(right)))
                        .hasType(INTEGER)
                        .isEqualTo(left);

                assertThat(assertions.expression("a IS DISTINCT FROM b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(!Objects.equals(left == null ? null : new BigDecimal(left), right));

                // arithmetic operators are already tested in TestDecimalOperators
            }
        }
    }

    @Test
    public void testBinaryOperatorsDecimalDouble()
    {
        for (BigDecimal left : decimalLefts) {
            for (Double right : doubleRights) {
                assertThat(assertions.expression("a = b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.doubleValue() == right);

                assertThat(assertions.expression("a <> b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.doubleValue() != right);

                assertThat(assertions.expression("a > b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.doubleValue() > right);

                assertThat(assertions.expression("a < b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.doubleValue() < right);

                assertThat(assertions.expression("a >= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.doubleValue() >= right);

                assertThat(assertions.expression("a <= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.doubleValue() <= right);

                assertThat(assertions.function("nullif", toLiteral(left), toLiteral(right)))
                        .isEqualTo(toSqlDecimal(BigDecimal.class.cast(nullIf(left, right))));

                assertThat(assertions.expression("a IS DISTINCT FROM b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(!Objects.equals(left == null ? null : left.doubleValue(), right));

                assertThat(assertions.expression("a + b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left.doubleValue() + right);

                assertThat(assertions.expression("a - b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left.doubleValue() - right);

                assertThat(assertions.expression("a * b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left.doubleValue() * right);

                assertThat(assertions.expression("a / b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left.doubleValue() / right);

                assertThat(assertions.expression("a % b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left.doubleValue() % right);
            }
        }
    }

    @Test
    public void testBinaryOperatorsDoubleDecimal()
    {
        for (Double left : doubleLefts) {
            for (BigDecimal right : decimalRights) {
                assertThat(assertions.expression("a = b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left == right.doubleValue());

                assertThat(assertions.expression("a <> b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left != right.doubleValue());

                assertThat(assertions.expression("a > b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left > right.doubleValue());

                assertThat(assertions.expression("a < b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left < right.doubleValue());

                assertThat(assertions.expression("a >= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left >= right.doubleValue());

                assertThat(assertions.expression("a <= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left <= right.doubleValue());

                assertThat(assertions.function("nullif", toLiteral(left), toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(nullIf(left, right));

                assertThat(assertions.expression("a IS DISTINCT FROM b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(!Objects.equals(left, right == null ? null : right.doubleValue()));

                assertThat(assertions.expression("a + b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left + right.doubleValue());

                assertThat(assertions.expression("a - b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left - right.doubleValue());

                assertThat(assertions.expression("a * b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left * right.doubleValue());

                assertThat(assertions.expression("a / b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left / right.doubleValue());

                assertThat(assertions.expression("a % b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : left % right.doubleValue());
            }
        }
    }

    @Test
    public void testBinaryOperatorsString()
    {
        for (String left : stringLefts) {
            for (String right : stringRights) {
                assertThat(assertions.expression("a = b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.equals(right));

                assertThat(assertions.expression("a <> b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : !left.equals(right));

                assertThat(assertions.expression("a > b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.compareTo(right) > 0);

                assertThat(assertions.expression("a < b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.compareTo(right) < 0);

                assertThat(assertions.expression("a >= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.compareTo(right) >= 0);

                assertThat(assertions.expression("a <= b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(left == null || right == null ? null : left.compareTo(right) <= 0);

                assertThat(assertions.expression("a || b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(VARCHAR)
                        .isEqualTo(left == null || right == null ? null : left + right);

                assertThat(assertions.expression("a IS DISTINCT FROM b")
                        .binding("a", toLiteral(left))
                        .binding("b", toLiteral(right)))
                        .hasType(BOOLEAN)
                        .isEqualTo(!Objects.equals(left, right));

                assertThat(assertions.function("nullif", toLiteral(left), toLiteral(right)))
                        .hasType(varcharType(left))
                        .isEqualTo(nullIf(left, right));
            }
        }
    }

    private static VarcharType varcharType(String... values)
    {
        return varcharType(Arrays.asList(values));
    }

    private static VarcharType varcharType(List<String> values)
    {
        if (values.stream().anyMatch(Objects::isNull)) {
            return VARCHAR;
        }
        return createVarcharType(values.stream().mapToInt(String::length).max().getAsInt());
    }

    private static Object nullIf(Object left, Object right)
    {
        if (left == null) {
            return null;
        }
        if (right == null) {
            return left;
        }

        if (left.equals(right)) {
            return null;
        }

        if ((left instanceof Double || right instanceof Double) && ((Number) left).doubleValue() == ((Number) right).doubleValue()) {
            return null;
        }

        return left;
    }

    @Test
    public void testTernaryOperatorsLongLong()
    {
        for (Integer first : intLefts) {
            for (Integer second : intLefts) {
                for (Integer third : intRights) {
                    assertThat(assertions.expression("value BETWEEN low AND high")
                            .binding("value", toLiteral(first))
                            .binding("low", toLiteral(second))
                            .binding("high", toLiteral(third)))
                            .isEqualTo(between(first, second, third, (min, value) -> min <= value, (value, max) -> value <= max));
                }
            }
        }
    }

    @Test
    public void testTernaryOperatorsLongDouble()
    {
        for (Integer first : intLefts) {
            for (Double second : doubleLefts) {
                for (Integer third : intRights) {
                    assertThat(assertions.expression("value BETWEEN low AND high")
                            .binding("value", toLiteral(first))
                            .binding("low", toLiteral(second))
                            .binding("high", toLiteral(third)))
                            .isEqualTo(between(first, second, third, (min, value) -> min <= value, (value, max) -> value <= max));
                }
            }
        }
    }

    @Test
    public void testTernaryOperatorsDoubleDouble()
    {
        for (Double first : doubleLefts) {
            for (Double second : doubleLefts) {
                for (Integer third : intRights) {
                    assertThat(assertions.expression("value BETWEEN low AND high")
                            .binding("value", toLiteral(first))
                            .binding("low", toLiteral(second))
                            .binding("high", toLiteral(third)))
                            .isEqualTo(between(first, second, third, (min, value) -> min <= value, (value, max) -> value <= max));
                }
            }
        }
    }

    @Test
    public void testTernaryOperatorsString()
    {
        for (String first : stringLefts) {
            for (String second : stringLefts) {
                for (String third : stringRights) {
                    assertThat(assertions.expression("value BETWEEN low AND high")
                            .binding("value", toLiteral(first))
                            .binding("low", toLiteral(second))
                            .binding("high", toLiteral(third)))
                            .isEqualTo(between(first, second, third, (min, value) -> min.compareTo(value) <= 0, (value, max) -> value.compareTo(max) <= 0));
                }
            }
        }
    }

    @Test
    public void testTernaryOperatorsLongDecimal()
    {
        for (Long first : longLefts) {
            for (BigDecimal second : decimalMiddle) {
                for (Long third : longRights) {
                    assertThat(assertions.expression("value BETWEEN low AND high")
                            .binding("value", toLiteral(first))
                            .binding("low", toLiteral(second))
                            .binding("high", toLiteral(third)))
                            .isEqualTo(between(first, second, third, (min, value) -> min.compareTo(new BigDecimal(value)) <= 0, (value, max) -> value <= max));
                }
            }
        }
    }

    @Test
    public void testTernaryOperatorsDecimalDouble()
    {
        for (BigDecimal first : decimalLefts) {
            for (Double second : doubleMiddle) {
                for (BigDecimal third : decimalRights) {
                    assertThat(assertions.expression("value BETWEEN low AND high")
                            .binding("value", toLiteral(first))
                            .binding("low", toLiteral(second))
                            .binding("high", toLiteral(third)))
                            .isEqualTo(between(first, second, third, (min, value) -> min <= value.doubleValue(), (value, max) -> value.compareTo(max) <= 0));
                }
            }
        }
    }

    private static <V, L, H> Boolean between(V value, L min, H max, BiPredicate<L, V> greaterThanOrEquals, BiPredicate<V, H> lessThanOrEquals)
    {
        if (value == null) {
            return null;
        }

        Boolean greaterOrEqualToMin = min == null ? null : greaterThanOrEquals.test(min, value);
        Boolean lessThanOrEqualToMax = max == null ? null : lessThanOrEquals.test(value, max);

        if (greaterOrEqualToMin == null) {
            return Objects.equals(lessThanOrEqualToMax, Boolean.FALSE) ? false : null;
        }
        if (lessThanOrEqualToMax == null) {
            return Objects.equals(greaterOrEqualToMin, Boolean.FALSE) ? false : null;
        }
        return greaterOrEqualToMin && lessThanOrEqualToMax;
    }

    @Test
    public void testCast()
    {
        for (Boolean value : booleanValues) {
            assertThat(assertions.expression("CAST(a AS boolean)")
                    .binding("a", toLiteral(value)))
                    .hasType(BOOLEAN)
                    .isEqualTo(value == null ? null : (value ? true : false));

            assertThat(assertions.expression("CAST(a AS integer)")
                    .binding("a", toLiteral(value)))
                    .hasType(INTEGER)
                    .isEqualTo(value == null ? null : (value ? 1 : 0));

            assertThat(assertions.expression("CAST(a AS bigint)")
                    .binding("a", toLiteral(value)))
                    .hasType(BIGINT)
                    .isEqualTo(value == null ? null : (value ? 1L : 0L));

            assertThat(assertions.expression("CAST(a AS double)")
                    .binding("a", toLiteral(value)))
                    .hasType(DOUBLE)
                    .isEqualTo(value == null ? null : (value ? 1.0 : 0.0));

            assertThat(assertions.expression("CAST(a AS varchar)")
                    .binding("a", toLiteral(value)))
                    .hasType(VARCHAR)
                    .isEqualTo(value == null ? null : (value ? "true" : "false"));
        }

        for (Integer value : intLefts) {
            assertThat(assertions.expression("CAST(a AS boolean)")
                    .binding("a", toLiteral(value)))
                    .hasType(BOOLEAN)
                    .isEqualTo(value == null ? null : (value != 0L ? true : false));

            assertThat(assertions.expression("CAST(a AS integer)")
                    .binding("a", toLiteral(value)))
                    .hasType(INTEGER)
                    .isEqualTo(value == null ? null : value);

            assertThat(assertions.expression("CAST(a AS bigint)")
                    .binding("a", toLiteral(value)))
                    .hasType(BIGINT)
                    .isEqualTo(value == null ? null : (long) value);

            assertThat(assertions.expression("CAST(a AS double)")
                    .binding("a", toLiteral(value)))
                    .hasType(DOUBLE)
                    .isEqualTo(value == null ? null : value.doubleValue());

            assertThat(assertions.expression("CAST(a AS varchar)")
                    .binding("a", toLiteral(value)))
                    .hasType(VARCHAR)
                    .isEqualTo(value == null ? null : String.valueOf(value));
        }

        DecimalFormat doubleFormat = new DecimalFormat("0.0###################E0");
        for (Double value : doubleLefts) {
            assertThat(assertions.expression("CAST(a AS boolean)")
                    .binding("a", toLiteral(value)))
                    .hasType(BOOLEAN)
                    .isEqualTo(value == null ? null : (value != 0.0 ? true : false));

            if (value == null || (value >= Long.MIN_VALUE && value < Long.MAX_VALUE)) {
                assertThat(assertions.expression("CAST(a AS bigint)")
                        .binding("a", toLiteral(value)))
                        .hasType(BIGINT)
                        .isEqualTo(value == null ? null : value.longValue());
            }

            assertThat(assertions.expression("CAST(a AS double)")
                    .binding("a", toLiteral(value)))
                    .hasType(DOUBLE)
                    .isEqualTo(value == null ? null : value);

            assertThat(assertions.expression("CAST(a AS varchar)")
                    .binding("a", toLiteral(value)))
                    .hasType(VARCHAR)
                    .isEqualTo(value == null ? null : doubleFormat.format(value));
        }

        assertThat(assertions.expression("CAST(a AS boolean)")
                .binding("a", "'true'"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(a AS BOOLEAN)")
                .binding("a", "'true'"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(a AS BOOLEAN)")
                .binding("a", "'tRuE'"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(a AS BOOLEAN)")
                .binding("a", "'false'"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("CAST(a AS BOOLEAN)")
                .binding("a", "'fAlSe'"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("CAST(a AS BOOLEAN)")
                .binding("a", "'t'"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(a AS BOOLEAN)")
                .binding("a", "'T'"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(a AS BOOLEAN)")
                .binding("a", "'f'"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("CAST(a AS BOOLEAN)")
                .binding("a", "'F'"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("CAST(a AS BOOLEAN)")
                .binding("a", "'1'"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(a AS BOOLEAN)")
                .binding("a", "'0'"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        for (Integer value : intLefts) {
            if (value != null) {
                assertThat(assertions.expression("CAST(a AS integer)")
                        .binding("a", toLiteral(String.valueOf(value))))
                        .hasType(INTEGER)
                        .isEqualTo(value == null ? null : value);

                assertThat(assertions.expression("CAST(a AS bigint)")
                        .binding("a", toLiteral(String.valueOf(value))))
                        .hasType(BIGINT)
                        .isEqualTo(value == null ? null : (long) value);
            }
        }
        for (Double value : doubleLefts) {
            if (value != null) {
                assertThat(assertions.expression("CAST(a AS double)")
                        .binding("a", toLiteral(String.valueOf(value))))
                        .hasType(DOUBLE)
                        .isEqualTo(value == null ? null : value);
            }
        }
        for (String value : stringLefts) {
            assertThat(assertions.expression("CAST(a AS varchar)")
                    .binding("a", toLiteral(value)))
                    .hasType(VARCHAR)
                    .isEqualTo(value == null ? null : value);
        }
    }

    @Test
    public void testTryCast()
    {
        assertThat(assertions.expression("try_CAST(a AS integer)")
                .binding("a", "null"))
                .isNull(INTEGER);

        assertThat(assertions.expression("try_CAST(a AS integer)")
                .binding("a", "'123'"))
                .hasType(INTEGER)
                .isEqualTo(123);

        assertThat(assertions.expression("try_CAST(a AS bigint)")
                .binding("a", "null"))
                .isNull(BIGINT);

        assertThat(assertions.expression("try_CAST(a AS bigint)")
                .binding("a", "'123'"))
                .hasType(BIGINT)
                .isEqualTo(123L);

        assertThat(assertions.expression("try_CAST(a AS varchar)")
                .binding("a", "'foo'"))
                .hasType(VARCHAR)
                .isEqualTo("foo");

        assertThat(assertions.expression("try_CAST(a AS bigint)")
                .binding("a", "'foo'"))
                .isNull(BIGINT);

        assertThat(assertions.expression("try_CAST(a AS integer)")
                .binding("a", "'foo'"))
                .isNull(INTEGER);

        assertThat(assertions.expression("try_CAST(a AS timestamp)")
                .binding("a", "'2001-08-22'"))
                .hasType(TIMESTAMP_MILLIS)
                .isEqualTo(sqlTimestampOf(3, 2001, 8, 22, 0, 0, 0, 0));

        assertThat(assertions.expression("try_CAST(a AS bigint)")
                .binding("a", "'hello'"))
                .isNull(BIGINT);

        assertThat(assertions.expression("try_CAST(a as bigint)")
                .binding("a", "CAST(null AS varchar)"))
                .isNull(BIGINT);

        assertThat(assertions.expression("try_CAST(a / 13 AS bigint)")
                .binding("a", "BIGINT '1234'"))
                .hasType(BIGINT)
                .isEqualTo(94L);

        assertThat(assertions.function("coalesce", "try_CAST('123' as bigint)", "456"))
                .hasType(BIGINT)
                .isEqualTo(123L);

        assertThat(assertions.function("coalesce", "try_CAST('foo' as bigint)", "456"))
                .hasType(BIGINT)
                .isEqualTo(456L);

        assertThat(assertions.expression("try_CAST(try_CAST(a AS varchar) as bigint)")
                .binding("a", "123"))
                .hasType(BIGINT)
                .isEqualTo(123L);

        assertThat(assertions.expression("try_CAST(a AS varchar) || try_CAST(b as varchar)")
                .binding("a", "'foo'")
                .binding("b", "'bar'"))
                .hasType(VARCHAR)
                .isEqualTo("foobar");
    }

    @Test
    public void testAnd()
    {
        assertThat(assertions.expression("a AND b")
                .binding("a", "true")
                .binding("b", "true"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a AND b")
                .binding("a", "true")
                .binding("b", "false"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a AND b")
                .binding("a", "false")
                .binding("b", "true"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a AND b")
                .binding("a", "false")
                .binding("b", "false"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a AND b")
                .binding("a", "true")
                .binding("b", "CAST(null as boolean)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a AND b")
                .binding("a", "false")
                .binding("b", "CAST(null as boolean)"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a AND b")
                .binding("a", "CAST(null as boolean)")
                .binding("b", "true"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a AND b")
                .binding("a", "CAST(null as boolean)")
                .binding("b", "false"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a AND b")
                .binding("a", "CAST(null as boolean)")
                .binding("b", "CAST(null as boolean)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a AND b")
                .binding("a", "true")
                .binding("b", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a AND b")
                .binding("a", "false")
                .binding("b", "null"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a AND b")
                .binding("a", "null")
                .binding("b", "true"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a AND b")
                .binding("a", "null")
                .binding("b", "false"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a AND b")
                .binding("a", "null")
                .binding("b", "null"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testOr()
    {
        assertThat(assertions.expression("a OR b")
                .binding("a", "true")
                .binding("b", "true"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a OR b")
                .binding("a", "true")
                .binding("b", "false"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a OR b")
                .binding("a", "false")
                .binding("b", "true"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a OR b")
                .binding("a", "false")
                .binding("b", "false"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a OR b")
                .binding("a", "true")
                .binding("b", "CAST(null as boolean)"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a OR b")
                .binding("a", "false")
                .binding("b", "CAST(null as boolean)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a OR b")
                .binding("a", "CAST(null as boolean)")
                .binding("b", "true"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a OR b")
                .binding("a", "CAST(null as boolean)")
                .binding("b", "false"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a OR b")
                .binding("a", "CAST(null as boolean)")
                .binding("b", "CAST(null as boolean)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a OR b")
                .binding("a", "true")
                .binding("b", "null"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a OR b")
                .binding("a", "false")
                .binding("b", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a OR b")
                .binding("a", "null")
                .binding("b", "true"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a OR b")
                .binding("a", "null")
                .binding("b", "false"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a OR b")
                .binding("a", "null")
                .binding("b", "null"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testNot()
    {
        assertThat(assertions.expression("NOT a")
                .binding("a", "true"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("NOT a")
                .binding("a", "false"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("NOT a")
                .binding("a", "CAST(null as boolean)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("NOT a")
                .binding("a", "null"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testIf()
    {
        assertThat(assertions.expression("if(a AND b, BIGINT '1', 0)")
                .binding("a", "null")
                .binding("b", "true"))
                .hasType(BIGINT)
                .isEqualTo(0L);

        assertThat(assertions.expression("if(a AND b, 1, 0)")
                .binding("a", "null")
                .binding("b", "true"))
                .hasType(INTEGER)
                .isEqualTo(0);

        for (Boolean condition : booleanValues) {
            for (String trueValue : stringLefts) {
                for (String falseValue : stringRights) {
                    assertThat(assertions.expression("if(a, b, c)")
                            .binding("a", toLiteral(condition))
                            .binding("b", toLiteral(trueValue))
                            .binding("c", toLiteral(falseValue)))
                            .hasType(varcharType(trueValue, falseValue))
                            .isEqualTo(condition != null && condition ? trueValue : falseValue);
                }
            }
        }
    }

    @Test
    public void testSimpleCase()
    {
        for (Double value : doubleLefts) {
            for (Double firstTest : doubleMiddle) {
                for (Double secondTest : doubleRights) {
                    String expected;
                    if (value == null) {
                        expected = "else";
                    }
                    else if (firstTest != null && (double) value == firstTest) {
                        expected = "first";
                    }
                    else if (secondTest != null && (double) value == secondTest) {
                        expected = "second";
                    }
                    else {
                        expected = "else";
                    }

                    assertThat(assertions.expression("case a when c1 then 'first' when c2 then 'second' else 'else' end")
                            .binding("a", toLiteral(value))
                            .binding("c1", toLiteral(firstTest))
                            .binding("c2", toLiteral(secondTest)))
                            .hasType(createVarcharType(6))
                            .isEqualTo(expected);
                }
            }
        }
        for (Integer value : intLefts) {
            for (Integer firstTest : intMiddle) {
                for (Integer secondTest : intRights) {
                    String expected;
                    if (value == null) {
                        expected = null;
                    }
                    else if (firstTest != null && firstTest.equals(value)) {
                        expected = "first";
                    }
                    else if (secondTest != null && secondTest.equals(value)) {
                        expected = "second";
                    }
                    else {
                        expected = null;
                    }

                    assertThat(assertions.expression("case a when c1 then 'first' when c2 then 'second' end")
                            .binding("a", toLiteral(value))
                            .binding("c1", toLiteral(firstTest))
                            .binding("c2", toLiteral(secondTest)))
                            .hasType(createVarcharType(6))
                            .isEqualTo(expected);
                }
            }
        }

        for (BigDecimal value : decimalLefts) {
            for (BigDecimal firstTest : decimalMiddle) {
                for (BigDecimal secondTest : decimalRights) {
                    String expected;
                    if (value == null) {
                        expected = null;
                    }
                    else if (firstTest != null && firstTest.equals(value)) {
                        expected = "first";
                    }
                    else if (secondTest != null && secondTest.equals(value)) {
                        expected = "second";
                    }
                    else {
                        expected = null;
                    }

                    assertThat(assertions.expression("case a when c1 then 'first' when c2 then 'second' end")
                            .binding("a", toLiteral(value))
                            .binding("c1", toLiteral(firstTest))
                            .binding("c2", toLiteral(secondTest)))
                            .hasType(createVarcharType(6))
                            .isEqualTo(expected);
                }
            }
        }

        assertThat(assertions.expression("CASE a WHEN b THEN 'matched' ELSE 'not_matched' END")
                .binding("a", "ARRAY[CAST(1 AS BIGINT)]")
                .binding("b", "ARRAY[CAST(1 AS BIGINT)]"))
                .hasType(createVarcharType(11))
                .isEqualTo("matched");

        assertThat(assertions.expression("CASE a WHEN b THEN 'matched' ELSE 'not_matched' END")
                .binding("a", "ARRAY[CAST(2 AS BIGINT)]")
                .binding("b", "ARRAY[CAST(1 AS BIGINT)]"))
                .hasType(createVarcharType(11))
                .isEqualTo("not_matched");

        assertThat(assertions.expression("CASE a WHEN b THEN 'matched' ELSE 'not_matched' END")
                .binding("a", "ARRAY[CAST(null AS BIGINT)]")
                .binding("b", "ARRAY[CAST(1 AS BIGINT)]"))
                .hasType(createVarcharType(11))
                .isEqualTo("not_matched");
    }

    @Test
    public void testSearchCaseSingle()
    {
        // assertExecute("case when null and true then 1 else 0 end", 0L);
        for (Double value : doubleLefts) {
            for (Integer firstTest : intLefts) {
                for (Double secondTest : doubleRights) {
                    String expected;
                    if (value == null) {
                        expected = "else";
                    }
                    else if (firstTest != null && (double) value == firstTest) {
                        expected = "first";
                    }
                    else if (secondTest != null && (double) value == secondTest) {
                        expected = "second";
                    }
                    else {
                        expected = "else";
                    }

                    assertThat(assertions.expression("case when v = c1 then 'first' when v = c2 then 'second' else 'else' end")
                            .binding("v", toLiteral(value))
                            .binding("c1", toLiteral(firstTest))
                            .binding("c2", toLiteral(secondTest)))
                            .hasType(createVarcharType(6))
                            .isEqualTo(expected);
                }
            }
        }

        for (Double value : doubleLefts) {
            for (Long firstTest : longLefts) {
                for (BigDecimal secondTest : decimalRights) {
                    String expected;
                    if (value == null) {
                        expected = "else";
                    }
                    else if (firstTest != null && (double) value == firstTest) {
                        expected = "first";
                    }
                    else if (secondTest != null && value == secondTest.doubleValue()) {
                        expected = "second";
                    }
                    else {
                        expected = "else";
                    }

                    assertThat(assertions.expression("case when v = c1 then 'first' when v = c2 then 'second' else 'else' end")
                            .binding("v", toLiteral(value))
                            .binding("c1", toLiteral(firstTest))
                            .binding("c2", toLiteral(secondTest)))
                            .hasType(createVarcharType(6))
                            .isEqualTo(expected);
                }
            }
        }

        assertThat(assertions.expression("CASE WHEN a = b THEN 'matched' ELSE 'not_matched' END")
                .binding("a", "ARRAY[CAST(1 AS BIGINT)]")
                .binding("b", "ARRAY[CAST(1 AS BIGINT)]"))
                .hasType(createVarcharType(11))
                .isEqualTo("matched");

        assertThat(assertions.expression("CASE WHEN a = b THEN 'matched' ELSE 'not_matched' END")
                .binding("a", "ARRAY[CAST(2 AS BIGINT)]")
                .binding("b", "ARRAY[CAST(1 AS BIGINT)]"))
                .hasType(createVarcharType(11))
                .isEqualTo("not_matched");

        assertThat(assertions.expression("CASE WHEN a = b THEN 'matched' ELSE 'not_matched' END")
                .binding("a", "ARRAY[CAST(null AS BIGINT)]")
                .binding("b", "ARRAY[CAST(1 AS BIGINT)]"))
                .hasType(createVarcharType(11))
                .isEqualTo("not_matched");
    }

    @Test
    public void testSearchCaseMultiple()
    {
        for (Double value : doubleLefts) {
            for (Integer firstTest : intLefts) {
                for (Double secondTest : doubleRights) {
                    String expected;
                    if (value == null) {
                        expected = null;
                    }
                    else if (firstTest != null && (double) value == firstTest) {
                        expected = "first";
                    }
                    else if (secondTest != null && (double) value == secondTest) {
                        expected = "second";
                    }
                    else {
                        expected = null;
                    }

                    assertThat(assertions.expression("case when v = c1 then 'first' when v = c2 then 'second' end")
                            .binding("v", toLiteral(value))
                            .binding("c1", toLiteral(firstTest))
                            .binding("c2", toLiteral(secondTest)))
                            .hasType(createVarcharType(6))
                            .isEqualTo(expected);
                }
            }
        }

        for (BigDecimal value : decimalLefts) {
            for (Long firstTest : longLefts) {
                for (Double secondTest : doubleRights) {
                    String expected;
                    if (value == null) {
                        expected = null;
                    }
                    else if (firstTest != null && value.doubleValue() == firstTest) {
                        expected = "first";
                    }
                    else if (secondTest != null && value.doubleValue() == secondTest) {
                        expected = "second";
                    }
                    else {
                        expected = null;
                    }

                    assertThat(assertions.expression("case when v = c1 then 'first' when v = c2 then 'second' end")
                            .binding("v", toLiteral(value))
                            .binding("c1", toLiteral(firstTest))
                            .binding("c2", toLiteral(secondTest)))
                            .hasType(createVarcharType(6))
                            .isEqualTo(expected);
                }
            }
        }
    }

    @Test
    public void testIn()
    {
        for (Boolean value : booleanValues) {
            assertThat(assertions.expression("a IN (true)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : value == Boolean.TRUE);
            assertThat(assertions.expression("a IN (null, true)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : value == Boolean.TRUE ? true : null);
            assertThat(assertions.expression("a IN (true, null)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : value == Boolean.TRUE ? true : null);
            assertThat(assertions.expression("a IN (false)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : value == Boolean.FALSE);
            assertThat(assertions.expression("a IN (null, false)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : value == Boolean.FALSE ? true : null);
            assertThat(assertions.expression("a IN (null)")
                    .binding("a", toLiteral(value)))
                    .isNull(BOOLEAN);
        }

        for (Integer value : intLefts) {
            List<Integer> testValues = Arrays.asList(33, 9, -9, -33);

            assertThat(assertions.expression("a IN (33, 9, -9, -33)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value));

            assertThat(assertions.expression("a IN (null, 33, 9, -9, -33)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            assertThat(assertions.expression("a IN (CAST(null AS BIGINT), 33, 9, -9, -33)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            assertThat(assertions.expression("a IN (33, null, 9, -9, -33)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            assertThat(assertions.expression("a IN (33, CAST(null AS BIGINT), 9, -9, -33)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            // compare a long to in containing doubles
            assertThat(assertions.expression("a IN (33, 9.0E0, -9, -33)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value));

            assertThat(assertions.expression("a IN (null, 33, 9.0E0, -9, -33)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            assertThat(assertions.expression("a IN (33.0E0, null, 9.0E0, -9, -33)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);
        }

        for (Double value : doubleLefts) {
            List<Double> testValues = Arrays.asList(33.0, 9.0, -9.0, -33.0);

            assertThat(assertions.expression("a IN (33.0E0, 9.0E0, -9.0E0, -33.0E0)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value));

            assertThat(assertions.expression("a IN (null, 33.0E0, 9.0E0, -9.0E0, -33.0E0)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            assertThat(assertions.expression("a IN (33.0E0, null, 9.0E0, -9.0E0, -33.0E0)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            // compare a double to in containing longs
            assertThat(assertions.expression("a IN (33.0E0, 9, -9, -33.0E0)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value));

            assertThat(assertions.expression("a IN (null, 33.0E0, 9, -9, -33.0E0)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            assertThat(assertions.expression("a IN (33.0E0, null, 9, -9, -33.0E0)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            // compare to dynamically computed values
            List<Double> cosines = Arrays.asList(33.0, cos(9.0), cos(-9.0), -33.0);
            assertThat(assertions.expression("cos(a) IN (33.0E0, cos(9.0E0), cos(-9.0E0), -33.0E0)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : cosines.contains(cos(value)));

            assertThat(assertions.expression("cos(a) IN (null, 33.0E0, cos(9.0E0), cos(-9.0E0), -33.0E0)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : cosines.contains(cos(value)) ? true : null);
        }

        for (BigDecimal value : decimalLefts) {
            List<BigDecimal> testValues = ImmutableList.of(new BigDecimal("9.0"), new BigDecimal("10.0"), new BigDecimal("-11.0"), new BigDecimal("9223372036.5477"));

            assertThat(assertions.expression("a IN (9.0, 10.0, -11.0, 9223372036.5477)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value));

            assertThat(assertions.expression("a IN (null, 9.0, 10.0, -11.0, 9223372036.5477)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            assertThat(assertions.expression("a IN (CAST(null AS DECIMAL(1,0)), 9.0, 10.0, -11.0, 9223372036.5477)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            assertThat(assertions.expression("a IN (9.0, CAST(null AS DECIMAL(1,0)), 10.0, -11.0, 9223372036.5477)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            assertThat(assertions.expression("a IN (9.0, null, 10.0, -11.0, 9223372036.5477)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            // compare a long to in containing doubles
            assertThat(assertions.expression("a IN (9.0, 10.0, CAST(-11.0 as DOUBLE), 9223372036.5477)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value));

            assertThat(assertions.expression("a IN (null, 9.0, 10.0, CAST(-11.0 as DOUBLE), 9223372036.5477)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            assertThat(assertions.expression("a IN (null, 9, 10.0, CAST(-11.0 as DOUBLE), 9223372036.5477)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            assertThat(assertions.expression("a IN (CAST(9.0 as DOUBLE), null, 10.0, CAST(-11.0 as DOUBLE), 9223372036.5477)")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);
        }

        for (String value : stringLefts) {
            List<String> testValues = Arrays.asList("what?", "foo", "mellow", "end");

            assertThat(assertions.expression("a IN ('what?', 'foo', 'mellow', 'end')")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value));

            assertThat(assertions.expression("a IN (null, 'what?', 'foo', 'mellow', 'end')")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);

            assertThat(assertions.expression("a IN ('what?', null, 'foo', 'mellow', 'end')")
                    .binding("a", toLiteral(value)))
                    .isEqualTo(value == null ? null : testValues.contains(value) ? true : null);
        }

        // Test null-handling in default case of InCodeGenerator
        assertThat(assertions.expression("a IN (100, 101, if(rand()>=0, 1), if(rand()<0, 1))")
                .binding("a", "1"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (100, 101, if(rand()<0, 1), if(rand()>=0, 1))")
                .binding("a", "1"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (100, 101, if(rand()>=0, 1), if(rand()<0, 1))")
                .binding("a", "2"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (100, 101, if(rand()<0, 1), if(rand()>=0, 1))")
                .binding("a", "2"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testHugeIn()
    {
        String intValues = range(2000, 7000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));

        assertThat(assertions.expression("a IN (1234, " + intValues + ")")
                .binding("a", "1234"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (" + intValues + ")")
                .binding("a", "1234"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        String longValues = LongStream.range(Integer.MAX_VALUE + 1L, Integer.MAX_VALUE + 5000L)
                .mapToObj(Long::toString)
                .collect(joining(", "));

        assertThat(assertions.expression("a IN (1234, " + longValues + ")")
                .binding("a", "BIGINT '1234'"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (" + longValues + ")")
                .binding("a", "BIGINT '1234'"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        String doubleValues = range(2000, 7000).asDoubleStream()
                .mapToObj(TestExpressionCompiler::formatDoubleToScientificNotation)
                .collect(joining(", "));

        assertThat(assertions.expression("a IN (12.34E0, " + doubleValues + ")")
                .binding("a", "12.34E0"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (" + doubleValues + ")")
                .binding("a", "12.34E0"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        String stringValues = range(2000, 7000)
                .mapToObj(i -> format("'%s'", i))
                .collect(joining(", "));

        assertThat(assertions.expression("a IN ('hello', " + stringValues + ")")
                .binding("a", "'hello'"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (" + stringValues + ")")
                .binding("a", "'hello'"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        String timestampValues = range(0, 2_000)
                .mapToObj(i -> format("TIMESTAMP '1970-01-01 01:01:0%s.%s+01:00'", i / 1000, i % 1000))
                .collect(joining(", "));

        assertThat(assertions.expression("a IN (" + timestampValues + ")")
                .binding("a", "TIMESTAMP '1970-01-01 00:01:00.999 UTC'"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (TIMESTAMP '1970-01-01 01:01:00.0+02:00')")
                .binding("a", "TIMESTAMP '1970-01-01 00:01:00.999'"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        String shortDecimalValues = range(2000, 7000)
                .mapToObj(value -> format("decimal '%s'", value))
                .collect(joining(", "));

        assertThat(assertions.expression("a IN (1234, " + shortDecimalValues + ")")
                .binding("a", "CAST(1234 AS decimal(14))"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (" + shortDecimalValues + ")")
                .binding("a", "CAST(1234 AS decimal(14))"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        String longDecimalValues = range(2000, 7000)
                .mapToObj(value -> format("decimal '123456789012345678901234567890%s'", value))
                .collect(joining(", "));

        assertThat(assertions.expression("a IN (1234, " + longDecimalValues + ")")
                .binding("a", "CAST(1234 AS decimal(28))"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (" + longDecimalValues + ")")
                .binding("a", "CAST(1234 AS decimal(28))"))
                .hasType(BOOLEAN)
                .isEqualTo(false);
    }

    @Test
    public void testInComplexTypes()
    {
        assertThat(assertions.expression("a IN (ARRAY[1])")
                .binding("a", "ARRAY[1]"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (ARRAY[2])")
                .binding("a", "ARRAY[1]"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a IN (ARRAY[2], ARRAY[1])")
                .binding("a", "ARRAY[1]"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (null)")
                .binding("a", "ARRAY[1]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (null, ARRAY[1])")
                .binding("a", "ARRAY[1]"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (ARRAY[2, null], ARRAY[1, null])")
                .binding("a", "ARRAY[1, 2, null]"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a IN (ARRAY[2, null], null)")
                .binding("a", "ARRAY[1, null]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ARRAY[null])")
                .binding("a", "ARRAY[null]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ARRAY[null])")
                .binding("a", "ARRAY[1]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ARRAY[1])")
                .binding("a", "ARRAY[null]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ARRAY[1, null])")
                .binding("a", "ARRAY[1, null]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ARRAY[2, null])")
                .binding("a", "ARRAY[1, null]"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a IN (ARRAY[1, null], ARRAY[2, null])")
                .binding("a", "ARRAY[1, null]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ARRAY[1, null], ARRAY[2, null], ARRAY[1, null])")
                .binding("a", "ARRAY[1, null]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ROW(1))")
                .binding("a", "ROW(1)"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (ROW(2))")
                .binding("a", "ROW(1)"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a IN (ROW(2), ROW(1), ROW(2))")
                .binding("a", "ROW(1)"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (null)")
                .binding("a", "ROW(1)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (null, ROW(1))")
                .binding("a", "ROW(1)"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (ROW(2, null), null)")
                .binding("a", "ROW(1, null)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ROW(null))")
                .binding("a", "ROW(null)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ROW(null))")
                .binding("a", "ROW(1)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ROW(1))")
                .binding("a", "ROW(null)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ROW(1, null))")
                .binding("a", "ROW(1, null)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ROW(2, null))")
                .binding("a", "ROW(1, null)"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a IN (ROW(1, null), ROW(2, null))")
                .binding("a", "ROW(1, null)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (ROW(1, null), ROW(2, null), ROW(1, null))")
                .binding("a", "ROW(1, null)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1], ARRAY[1]))")
                .binding("a", "MAP(ARRAY[1], ARRAY[1])"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (null)")
                .binding("a", "MAP(ARRAY[1], ARRAY[1])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (null, MAP(ARRAY[1], ARRAY[1]))")
                .binding("a", "MAP(ARRAY[1], ARRAY[1])"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1, 2], ARRAY[1, null]))")
                .binding("a", "MAP(ARRAY[1], ARRAY[1])"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1, 2], ARRAY[2, null]), null)")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[1, null])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1, 2], ARRAY[1, null]))")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[1, null])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1, 3], ARRAY[1, null]))")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[1, null])"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1], ARRAY[null]))")
                .binding("a", "MAP(ARRAY[1], ARRAY[null])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1], ARRAY[null]))")
                .binding("a", "MAP(ARRAY[1], ARRAY[1])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1], ARRAY[1]))")
                .binding("a", "MAP(ARRAY[1], ARRAY[null])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1, 2], ARRAY[1, null]))")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[1, null])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1, 3], ARRAY[1, null]))")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[1, null])"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1, 2], ARRAY[2, null]))")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[1, null])"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1, 2], ARRAY[1, null]), MAP(ARRAY[1, 2], ARRAY[2, null]))")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[1, null])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a IN (MAP(ARRAY[1, 2], ARRAY[1, null]), MAP(ARRAY[1, 2], ARRAY[2, null]), MAP(ARRAY[1, 2], ARRAY[1, null]))")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[1, null])"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testFunctionCall()
    {
        for (Integer left : intLefts) {
            for (Integer right : intRights) {
                assertThat(assertions.function("bitwise_and", toLiteral(left), toLiteral(right)))
                        .hasType(BIGINT)
                        .isEqualTo(left == null || right == null ? null : BitwiseFunctions.bitwiseAnd(left, right));
            }
        }

        for (Integer left : intLefts) {
            for (Double right : doubleRights) {
                assertThat(assertions.function("mod", toLiteral(left), toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : MathFunctions.mod(left, right));
            }
        }

        for (Double left : doubleLefts) {
            for (Integer right : intRights) {
                assertThat(assertions.function("mod", toLiteral(left), toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : MathFunctions.mod(left, right));
            }
        }

        for (Double left : doubleLefts) {
            for (Double right : doubleRights) {
                assertThat(assertions.function("mod", toLiteral(left), toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : MathFunctions.mod(left, right));
            }
        }

        for (Double left : doubleLefts) {
            for (BigDecimal right : decimalRights) {
                assertThat(assertions.function("mod", toLiteral(left), toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : MathFunctions.mod(left, right.doubleValue()));
            }
        }

        for (BigDecimal left : decimalLefts) {
            for (Long right : longRights) {
                assertThat(assertions.function("power", toLiteral(left), toLiteral(right)))
                        .hasType(DOUBLE)
                        .isEqualTo(left == null || right == null ? null : MathFunctions.power(left.doubleValue(), right));
            }
        }

        for (String value : stringLefts) {
            for (Integer start : intLefts) {
                for (Integer length : intRights) {
                    assertThat(assertions.function("substr", toLiteral(value), toLiteral(start), toLiteral(length)))
                            .hasType(value != null ? createVarcharType(value.length()) : VARCHAR)
                            .isEqualTo(value == null || start == null || length == null ? null : toString(StringFunctions.substring(utf8Slice(value), start, length)));
                }
            }
        }
    }

    @Test
    public void testFunctionCallRegexp()
    {
        for (String value : stringLefts) {
            for (String pattern : stringRights) {
                assertThat(assertions.function("regexp_like", toLiteral(value), toLiteral(pattern)))
                        .hasType(BOOLEAN)
                        .isEqualTo(value == null || pattern == null ? null : JoniRegexpFunctions.regexpLike(utf8Slice(value), joniRegexp(utf8Slice(pattern))));

                assertThat(assertions.function("regexp_replace", toLiteral(value), toLiteral(pattern)))
                        .hasType(value == null ? VARCHAR : createVarcharType(value.length()))
                        .isEqualTo(value == null || pattern == null ? null : JoniRegexpFunctions.regexpReplace(utf8Slice(value), joniRegexp(utf8Slice(pattern))).toStringUtf8());

                assertThat(assertions.function("regexp_extract", toLiteral(value), toLiteral(pattern)))
                        .hasType(value == null ? VARCHAR : createVarcharType(value.length()))
                        .isEqualTo(value == null || pattern == null ? null : toString(JoniRegexpFunctions.regexpExtract(utf8Slice(value), joniRegexp(utf8Slice(pattern)))));
            }
        }
    }

    @Test
    public void testFunctionCallJson()
    {
        String[] jsonValues = {
                "{}",
                "{\"fuu\": {\"bar\": 1}}",
                "{\"fuu\": null}",
                "{\"fuu\": 1}",
                "{\"fuu\": 1, \"bar\": \"abc\"}",
                null
        };

        String[] jsonPatterns = {
                "$",
                "$.fuu",
                "$.fuu[0]",
                "$.bar",
                null
        };

        for (String value : jsonValues) {
            for (String pattern : jsonPatterns) {
                assertThat(assertions.function("json_extract", toLiteral(value), toLiteral(pattern)))
                        .hasType(JSON)
                        .isEqualTo(value == null || pattern == null ? null : toString(JsonFunctions.jsonExtract(utf8Slice(value), new JsonPath(pattern))));

                assertThat(assertions.function("json_extract_scalar", toLiteral(value), toLiteral(pattern)))
                        .hasType(value == null ? createUnboundedVarcharType() : createVarcharType(value.length()))
                        .isEqualTo(value == null || pattern == null ? null : toString(JsonFunctions.jsonExtractScalar(utf8Slice(value), new JsonPath(pattern))));
            }
        }

        assertThat(assertions.function("json_array_contains", "'[1, 2, 3]'", "2"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[1, 2, 3]'", "BIGINT '2'"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[2.5E0]'", "2.5E0"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[false, true]'", "true"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[5]'", "3"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'['", "9"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_length", "'['"))
                .isNull(BIGINT);
    }

    @Test
    public void testFunctionWithSessionCall()
    {
        Session session = assertions.getDefaultSession();

        assertThat(assertions.expression("now()"))
                .hasType(TIMESTAMP_TZ_MILLIS)
                .isEqualTo(SqlTimestampWithTimeZone.fromInstant(3, session.getStart(), session.getTimeZoneKey().getZoneId()));

        assertThat(assertions.expression("current_timestamp"))
                .hasType(TIMESTAMP_TZ_MILLIS)
                .isEqualTo(SqlTimestampWithTimeZone.fromInstant(3, session.getStart(), session.getTimeZoneKey().getZoneId()));
    }

    @Test
    public void testExtract()
    {
        DateTime[] dateTimeValues = {
                new DateTime(2001, 1, 22, 3, 4, 5, 321, UTC),
                new DateTime(1960, 1, 22, 3, 4, 5, 321, UTC),
                new DateTime(1970, 1, 1, 0, 0, 0, 0, UTC),
                null
        };

        for (DateTime left : dateTimeValues) {
            for (Field field : Field.values()) {
                if (field == TIMEZONE_MINUTE || field == TIMEZONE_HOUR) {
                    continue;
                }
                Long expected;
                Long micros;
                if (left != null) {
                    micros = left.getMillis() * MICROSECONDS_PER_MILLISECOND;
                    expected = callExtractFunction(micros, field);
                }
                else {
                    micros = null;
                    expected = null;
                }

                assertThat(assertions.expression("extract(%s from from_unixtime(CAST(a as double) / 1000000, 'UTC'))".formatted(field))
                        .binding("a", toLiteral(micros)))
                        .isEqualTo(expected);
            }
        }
    }

    @SuppressWarnings("fallthrough")
    private static long callExtractFunction(long value, Field field)
    {
        return switch (field) {
            case YEAR -> ExtractYear.extract(value);
            case QUARTER -> ExtractQuarter.extract(value);
            case MONTH -> ExtractMonth.extract(value);
            case WEEK -> ExtractWeekOfYear.extract(value);
            case DAY, DAY_OF_MONTH -> ExtractDay.extract(value);
            case DAY_OF_WEEK, DOW -> ExtractDayOfWeek.extract(value);
            case YEAR_OF_WEEK, YOW -> ExtractYearOfWeek.extract(value);
            case DAY_OF_YEAR, DOY -> ExtractDayOfYear.extract(value);
            case HOUR -> ExtractHour.extract(value);
            case MINUTE -> ExtractMinute.extract(value);
            case SECOND -> ExtractSecond.extract(value);
            case TIMEZONE_MINUTE, TIMEZONE_HOUR -> throw new AssertionError("Unhandled field: " + field);
        };
    }

    @Test
    public void testLike()
    {
        for (String value : stringLefts) {
            for (String pattern : stringLefts) {
                assertThat(assertions.expression("a LIKE b")
                        .binding("a", toLiteral(value))
                        .binding("b", toLiteral(pattern)))
                        .isEqualTo(value == null || pattern == null ?
                                null :
                                LikeFunctions.likeVarchar(utf8Slice(value), LikeFunctions.likePattern(utf8Slice(pattern), utf8Slice("\\"))));
            }
        }
    }

    @Test
    public void testCoalesce()
    {
        assertThat(assertions.function("coalesce", "9", "1"))
                .hasType(INTEGER)
                .isEqualTo(9);

        assertThat(assertions.function("coalesce", "9", "null"))
                .hasType(INTEGER)
                .isEqualTo(9);

        assertThat(assertions.function("coalesce", "9", "BIGINT '1'"))
                .hasType(BIGINT)
                .isEqualTo(9L);

        assertThat(assertions.function("coalesce", "BIGINT '9'", "null"))
                .hasType(BIGINT)
                .isEqualTo(9L);

        assertThat(assertions.function("coalesce", "9", "CAST(null as bigint)"))
                .hasType(BIGINT)
                .isEqualTo(9L);

        assertThat(assertions.function("coalesce", "null", "9", "1"))
                .hasType(INTEGER)
                .isEqualTo(9);

        assertThat(assertions.function("coalesce", "null", "9", "null"))
                .hasType(INTEGER)
                .isEqualTo(9);

        assertThat(assertions.function("coalesce", "null", "9", "BIGINT '1'"))
                .hasType(BIGINT)
                .isEqualTo(9L);

        assertThat(assertions.function("coalesce", "null", "9", "CAST (null AS BIGINT)"))
                .hasType(BIGINT)
                .isEqualTo(9L);

        assertThat(assertions.function("coalesce", "null", "9", "CAST(null as bigint)"))
                .hasType(BIGINT)
                .isEqualTo(9L);

        assertThat(assertions.function("coalesce", "CAST(null as bigint)", "9", "1"))
                .hasType(BIGINT)
                .isEqualTo(9L);

        assertThat(assertions.function("coalesce", "CAST(null as bigint)", "9", "null"))
                .hasType(BIGINT)
                .isEqualTo(9L);

        assertThat(assertions.function("coalesce", "CAST(null as bigint)", "9", "CAST(null as bigint)"))
                .hasType(BIGINT)
                .isEqualTo(9L);

        assertThat(assertions.function("coalesce", "9.0E0", "1.0E0"))
                .hasType(DOUBLE)
                .isEqualTo(9.0);

        assertThat(assertions.function("coalesce", "9.0E0", "1"))
                .hasType(DOUBLE)
                .isEqualTo(9.0);

        assertThat(assertions.function("coalesce", "9.0E0", "null"))
                .hasType(DOUBLE)
                .isEqualTo(9.0);

        assertThat(assertions.function("coalesce", "9.0E0", "CAST(null as double)"))
                .hasType(DOUBLE)
                .isEqualTo(9.0);

        assertThat(assertions.function("coalesce", "null", "9.0E0", "1"))
                .hasType(DOUBLE)
                .isEqualTo(9.0);

        assertThat(assertions.function("coalesce", "null", "9.0E0", "null"))
                .hasType(DOUBLE)
                .isEqualTo(9.0);

        assertThat(assertions.function("coalesce", "null", "9.0E0", "CAST(null as double)"))
                .hasType(DOUBLE)
                .isEqualTo(9.0);

        assertThat(assertions.function("coalesce", "null", "9.0E0", "CAST(null as bigint)"))
                .hasType(DOUBLE)
                .isEqualTo(9.0);

        assertThat(assertions.function("coalesce", "CAST(null as bigint)", "9.0E0", "1"))
                .hasType(DOUBLE)
                .isEqualTo(9.0);

        assertThat(assertions.function("coalesce", "CAST(null as bigint)", "9.0E0", "null"))
                .hasType(DOUBLE)
                .isEqualTo(9.0);

        assertThat(assertions.function("coalesce", "CAST(null as bigint)", "9.0E0", "CAST(null as bigint)"))
                .hasType(DOUBLE)
                .isEqualTo(9.0);

        assertThat(assertions.function("coalesce", "CAST(null as double)", "9.0E0", "CAST(null as double)"))
                .hasType(DOUBLE)
                .isEqualTo(9.0);

        assertThat(assertions.function("coalesce", "'foo'", "'banana'"))
                .hasType(createVarcharType(6))
                .isEqualTo("foo");

        assertThat(assertions.function("coalesce", "'foo'", "null"))
                .hasType(createVarcharType(3))
                .isEqualTo("foo");

        assertThat(assertions.function("coalesce", "'foo'", "CAST(null as varchar)"))
                .hasType(VARCHAR)
                .isEqualTo("foo");

        assertThat(assertions.function("coalesce", "null", "'foo'", "'banana'"))
                .hasType(createVarcharType(6))
                .isEqualTo("foo");

        assertThat(assertions.function("coalesce", "null", "'foo'", "null"))
                .hasType(createVarcharType(3))
                .isEqualTo("foo");

        assertThat(assertions.function("coalesce", "null", "'foo'", "CAST(null as varchar)"))
                .hasType(VARCHAR)
                .isEqualTo("foo");

        assertThat(assertions.function("coalesce", "CAST(null as varchar)", "'foo'", "'bar'"))
                .hasType(VARCHAR)
                .isEqualTo("foo");

        assertThat(assertions.function("coalesce", "CAST(null as varchar)", "'foo'", "null"))
                .hasType(VARCHAR)
                .isEqualTo("foo");

        assertThat(assertions.function("coalesce", "CAST(null as varchar)", "'foo'", "CAST(null as varchar)"))
                .hasType(VARCHAR)
                .isEqualTo("foo");

        assertThat(assertions.function("coalesce", "CAST(null as bigint)", "null", "CAST(null as bigint)"))
                .isNull(BIGINT);
    }

    @Test
    public void testNullif()
    {
        assertThat(assertions.function("nullif", "NULL", "NULL"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("nullif", "NULL", "2"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("nullif", "2", "NULL"))
                .hasType(INTEGER)
                .isEqualTo(2);

        assertThat(assertions.function("nullif", "BIGINT '2'", "NULL"))
                .hasType(BIGINT)
                .isEqualTo(2L);

        assertThat(assertions.function("nullif", "ARRAY[CAST(1 AS BIGINT)]", "ARRAY[CAST(1 AS BIGINT)]"))
                .isNull(new ArrayType(BIGINT));

        assertThat(assertions.function("nullif", "ARRAY[CAST(1 AS BIGINT)]", "ARRAY[CAST(NULL AS BIGINT)]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(1L));

        assertThat(assertions.function("nullif", "ARRAY[CAST(NULL AS BIGINT)]", "ARRAY[CAST(NULL AS BIGINT)]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(singletonList(null));

        // Test coercion in which the CAST function takes ConnectorSession (e.g. MapToMapCast)
        assertThat(assertions.function("nullif", "map(array[1], array[smallint '1'])", "map(array[1], array[integer '1'])"))
                .isNull(mapType(INTEGER, SMALLINT));
    }

    private static String formatDoubleToScientificNotation(Double value)
    {
        DecimalFormat formatter = ((DecimalFormat) NumberFormat.getNumberInstance(Locale.US));
        formatter.applyPattern("0.##############E0");
        return formatter.format(value);
    }

    private static SqlDecimal toSqlDecimal(BigDecimal decimal)
    {
        if (decimal == null) {
            return null;
        }
        return new SqlDecimal(decimal.unscaledValue(), decimal.precision(), decimal.scale());
    }

    private static Type getDecimalType(BigDecimal decimal)
    {
        if (decimal == null) {
            return createDecimalType(1, 0);
        }
        return createDecimalType(decimal.precision(), decimal.scale());
    }

    private static String toLiteral(Boolean value)
    {
        return toLiteral(value, BOOLEAN);
    }

    private static String toLiteral(Long value)
    {
        return toLiteral(value, BIGINT);
    }

    private static String toLiteral(BigDecimal value)
    {
        return toLiteral(value, getDecimalType(value));
    }

    private static String toLiteral(Double value)
    {
        return toLiteral(value, DOUBLE);
    }

    private static String toLiteral(String value)
    {
        return toLiteral(value, VARCHAR);
    }

    private static String toLiteral(Integer value)
    {
        return toLiteral(value, INTEGER);
    }

    private static String toLiteral(Object value, Type type)
    {
        if (value == null) {
            value = "CAST(null AS %s)".formatted(type);
        }
        else if (type.equals(VARCHAR)) {
            value = "'%s'".formatted(value);
        }
        else if (type.equals(BIGINT)) {
            value = "BIGINT '%s'".formatted(value);
        }
        else if (type.equals(DOUBLE)) {
            value = "DOUBLE '%s'".formatted(value);
        }

        return String.valueOf(value);
    }

    private static String toString(Slice value)
    {
        return value == null ? null : value.toStringUtf8();
    }
}
