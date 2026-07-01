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
package io.trino.operator.scalar.interval;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestIntervalConversions
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void tearDown()
    {
        assertions.close();
    }

    @Test
    public void testNumericCastsHonorTargetPrecision()
    {
        for (String field : new String[] {"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"}) {
            for (String numeric : new String[] {"INTEGER", "BIGINT"}) {
                for (long value : new long[] {-99, -5, 0, 5, 99}) {
                    assertThat(assertions.expression("CAST(CAST(%s '%s' AS INTERVAL %s) AS BIGINT)".formatted(numeric, value, field))).isEqualTo(value);
                }
                for (long value : new long[] {-100, 100}) {
                    assertTrinoExceptionThrownBy(assertions.expression("CAST(%s '%s' AS INTERVAL %s)".formatted(numeric, value, field))::evaluate)
                            .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
                    assertThat(assertions.expression("CAST(CAST(%s '%s' AS INTERVAL %s(3)) AS BIGINT)".formatted(numeric, value, field))).isEqualTo(value);
                }
            }
        }
        for (int precision = 0; precision <= 12; precision++) {
            assertThat(assertions.expression("CAST(CAST(5 AS INTERVAL SECOND(2,%s)) AS BIGINT)".formatted(precision))).isEqualTo(5L);
            assertThat(assertions.expression("CAST(CAST(BIGINT '-5' AS INTERVAL SECOND(2,%s)) AS BIGINT)".formatted(precision))).isEqualTo(-5L);
            assertTrinoExceptionThrownBy(assertions.expression("CAST(100 AS INTERVAL SECOND(2,%s))".formatted(precision))::evaluate)
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        }
    }

    @Test
    public void testNarrowTrailingFields()
    {
        for (int sign : new int[] {-1, 1}) {
            assertThat(assertions.expression("CAST(CAST(INTERVAL '%s' HOUR AS INTERVAL DAY) AS INTERVAL HOUR) = INTERVAL '%s' HOUR".formatted(sign * 25, sign * 24))).isEqualTo(true);
            assertThat(assertions.expression("CAST(CAST(INTERVAL '%s' MONTH AS INTERVAL YEAR) AS INTERVAL MONTH) = INTERVAL '%s' MONTH".formatted(sign * 13, sign * 12))).isEqualTo(true);
        }
        for (String source : new String[] {"SECOND(6,6)", "SECOND(6,12)"}) {
            for (String target : new String[] {"DAY", "HOUR", "MINUTE(4)", "DAY TO HOUR", "DAY TO MINUTE", "HOUR TO MINUTE"}) {
                assertThat(assertions.expression("CAST(CAST(CAST(INTERVAL '90061.999999' %s AS INTERVAL %s) AS INTERVAL SECOND(13,6)) AS BIGINT)".formatted(source, target)))
                        .isEqualTo(switch (target) {
                            case "DAY" -> 86400L;
                            case "HOUR", "DAY TO HOUR" -> 90000L;
                            default -> 90060L;
                        });
            }
        }
        assertThat(assertions.expression("CAST(INTERVAL '-23:59:59.999999999999' HOUR TO SECOND(12) AS INTERVAL DAY) = INTERVAL '0' DAY")).isEqualTo(true);
        assertThat(assertions.expression("CAST(INTERVAL '-59.999999999999' SECOND(2,12) AS INTERVAL MINUTE) = INTERVAL '0' MINUTE")).isEqualTo(true);
    }

    @Test
    public void testQualifierAwareVarcharRoundTrip()
    {
        for (String qualifier : new String[] {"YEAR", "MONTH", "YEAR TO MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "DAY TO HOUR", "DAY TO MINUTE", "DAY TO SECOND", "HOUR TO MINUTE", "HOUR TO SECOND", "MINUTE TO SECOND", "SECOND(2,12)", "DAY TO SECOND(12)", "HOUR TO SECOND(12)", "MINUTE TO SECOND(12)"}) {
            for (String sign : new String[] {"", "-"}) {
                String value = switch (qualifier) {
                    case "YEAR TO MONTH" -> "5-11";
                    case "DAY TO HOUR" -> "5 23";
                    case "DAY TO MINUTE" -> "5 23:59";
                    case "DAY TO SECOND" -> "5 23:59:59.123456";
                    case "HOUR TO MINUTE" -> "5:59";
                    case "HOUR TO SECOND" -> "5:59:59.123456";
                    case "MINUTE TO SECOND" -> "5:59.123456";
                    case "SECOND" -> "5.123456";
                    case "SECOND(2,12)" -> "5.999999999999";
                    case "DAY TO SECOND(12)" -> "5 23:59:59.999999999999";
                    case "HOUR TO SECOND(12)" -> "5:59:59.999999999999";
                    case "MINUTE TO SECOND(12)" -> "5:59.999999999999";
                    default -> "5";
                };
                String literal = "INTERVAL '%s%s' %s".formatted(sign, value, qualifier);
                assertThat(assertions.expression("CAST(%s AS VARCHAR)".formatted(literal))).isEqualTo(sign + value);
                assertThat(assertions.expression("CAST(CAST(%s AS VARCHAR) AS INTERVAL %s) = %s".formatted(literal, qualifier, literal))).isEqualTo(true);
            }
        }
        assertThat(assertions.expression("CAST(INTERVAL '-0.000000000001' SECOND(2,12) AS VARCHAR)")).isEqualTo("-0.000000000001");
        assertThat(assertions.expression("CAST(INTERVAL '-9223372036854.775808' SECOND(13,6) AS VARCHAR)")).isEqualTo("-9223372036854.775808");
        for (String value : new String[] {"-9223372036854.775808", "9223372036854.775807", "-9223372036854.775807999999", "9223372036854.775807999999"}) {
            assertThat(assertions.expression("CAST(CAST(INTERVAL '%s' SECOND(13,12) AS VARCHAR) AS INTERVAL SECOND(13,12)) = INTERVAL '%s' SECOND(13,12)".formatted(value, value))).isEqualTo(true);
        }
        assertThat(assertions.expression("CAST(CAST(INTERVAL '-2147483648' MONTH(10) AS VARCHAR) AS INTERVAL MONTH(10)) = INTERVAL '-2147483648' MONTH(10)")).isEqualTo(true);
    }

    @Test
    public void testScalarArithmeticPrecision()
    {
        for (int precision = 0; precision <= 6; precision++) {
            String value = precision == 0 ? "3" : "0." + "0".repeat(precision - 1) + "3";
            String expected = precision == 0 ? "2" : "0." + "0".repeat(precision - 1) + "2";
            String negativeExpected = precision == 0 ? "-1" : "-0." + "0".repeat(precision - 1) + "1";
            for (String operation : new String[] {"v / 2", "v * DOUBLE '0.5'", "DOUBLE '0.5' * v"}) {
                assertThat(assertions.expression("(%s) = INTERVAL '%s' SECOND(2,%s)".formatted(operation, expected, precision))
                        .binding("v", "INTERVAL '%s' SECOND(2,%s)".formatted(value, precision))).isEqualTo(true);
                assertThat(assertions.expression("CAST(%s AS VARCHAR)".formatted(operation))
                        .binding("v", "INTERVAL '%s' SECOND(2,%s)".formatted(value, precision))).isEqualTo(expected);
                assertThat(assertions.expression("(%s) = INTERVAL '%s' SECOND(2,%s)".formatted(operation, negativeExpected, precision))
                        .binding("v", "INTERVAL '-%s' SECOND(2,%s)".formatted(value, precision))).isEqualTo(true);
            }
        }
        assertThat(assertions.expression("INTERVAL '3' HOUR / 2 = INTERVAL '1' HOUR")).isEqualTo(true);
        for (String operation : new String[] {"v / 2", "v * DOUBLE '0.5'", "DOUBLE '0.5' * v"}) {
            assertThat(assertions.expression("(%s) = INTERVAL '1' YEAR".formatted(operation)).binding("v", "INTERVAL '3' YEAR")).isEqualTo(true);
            assertThat(assertions.expression("CAST(%s AS VARCHAR)".formatted(operation)).binding("v", "INTERVAL '3' YEAR")).isEqualTo("1");
        }
    }

    @Test
    public void testExtractNegativeLongIntervalBoundaries()
    {
        for (String qualifier : new String[] {"DAY TO SECOND(12)", "HOUR TO SECOND(12)", "MINUTE TO SECOND(12)", "SECOND(6,12)"}) {
            String value = switch (qualifier) {
                case "DAY TO SECOND(12)" -> "-0 23:59:59.999999999999";
                case "HOUR TO SECOND(12)" -> "-23:59:59.999999999999";
                case "MINUTE TO SECOND(12)" -> "-1439:59.999999999999";
                default -> "-86399.999999999999";
            };
            long second = qualifier.startsWith("SECOND") ? -86399 : -59;
            assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '%s' %s)".formatted(value, qualifier))).isEqualTo(second);
            if (!qualifier.startsWith("SECOND")) {
                assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '%s' %s)".formatted(value, qualifier))).isEqualTo(qualifier.startsWith("MINUTE") ? -1439L : -59L);
            }
            if (qualifier.startsWith("DAY") || qualifier.startsWith("HOUR")) {
                assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '%s' %s)".formatted(value, qualifier))).isEqualTo(-23L);
            }
        }
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '-0 23:59:59.999999999999' DAY TO SECOND(12))")).isEqualTo(0L);
        assertThat(assertions.expression("millisecond(INTERVAL '-0.000999999999' SECOND(2,12))")).isEqualTo(0L);
    }

    @Test
    public void testRoundingOverflow()
    {
        for (String value : new String[] {"9223372036854.775807", "-9223372036854.775808"}) {
            for (int precision : new int[] {6, 12}) {
                assertTrinoExceptionThrownBy(assertions.expression("CAST(INTERVAL '%s' SECOND(13,%s) AS INTERVAL SECOND(13,5))".formatted(value, precision))::evaluate)
                        .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            }
            assertTrinoExceptionThrownBy(assertions.expression("CAST('%s' AS INTERVAL SECOND(13,5))".formatted(value))::evaluate)
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            assertTrinoExceptionThrownBy(assertions.expression("INTERVAL '%s' SECOND(13,5)".formatted(value))::evaluate)
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        }
    }

    @Test
    public void testCommonTypePreservesCrossFieldRange()
    {
        for (String sign : new String[] {"", "-"}) {
            assertThat(assertions.expression("CAST(coalesce(INTERVAL '%s9999' MINUTE, INTERVAL '0' HOUR) AS VARCHAR)".formatted(sign)))
                    .isEqualTo(sign + "166:39");
            assertThat(assertions.expression("CAST(coalesce(INTERVAL '%s9999' MONTH, INTERVAL '0' YEAR) AS VARCHAR)".formatted(sign)))
                    .isEqualTo(sign + "833-3");
            assertThat(assertions.expression("CAST(coalesce(INTERVAL '%s999' HOUR, INTERVAL '0' DAY) AS VARCHAR)".formatted(sign)))
                    .isEqualTo(sign + "41 15");
            assertThat(assertions.expression("CAST(coalesce(INTERVAL '%s999999.999999999999' SECOND(6,12), INTERVAL '0' DAY) AS VARCHAR)".formatted(sign)))
                    .isEqualTo(sign + "11 13:46:39.999999999999");
        }
    }

    @Test
    public void testLongScalarArithmeticRoundsOnce()
    {
        for (int precision = 7; precision <= 12; precision++) {
            String unit = "0." + "0".repeat(precision - 1) + "1";
            for (String sign : new String[] {"", "-"}) {
                for (String operation : new String[] {"v * DOUBLE '0.46'", "DOUBLE '0.46' * v", "v / DOUBLE '2.2'"}) {
                    assertThat(assertions.expression("(%s) = INTERVAL '0' SECOND(1,%s)".formatted(operation, precision))
                            .binding("v", "INTERVAL '%s%s' SECOND(1,%s)".formatted(sign, unit, precision))).isEqualTo(true);
                }
                for (String operation : new String[] {"v * DOUBLE '0.5'", "v / DOUBLE '2'"}) {
                    assertThat(assertions.expression("(%s) = INTERVAL '%s' SECOND(1,%s)".formatted(operation, sign.isEmpty() ? unit : "0", precision))
                            .binding("v", "INTERVAL '%s%s' SECOND(1,%s)".formatted(sign, unit, precision))).isEqualTo(true);
                }
                assertThat(assertions.expression("(v * DOUBLE '0.54') = v")
                        .binding("v", "INTERVAL '%s%s' SECOND(1,%s)".formatted(sign, unit, precision))).isEqualTo(true);
            }
        }
        for (String operation : new String[] {"v * DOUBLE '2'", "v / DOUBLE '0.5'"}) {
            for (String sign : new String[] {"", "-"}) {
                assertTrinoExceptionThrownBy(assertions.expression(operation)
                        .binding("v", "INTERVAL '%s9223372036854' SECOND(13,12)".formatted(sign))::evaluate)
                        .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            }
        }
    }

    @Test
    public void testFractionRequiresDigitsThroughout()
    {
        for (String value : new String[] {"1.-1", "1.+1", "1.123456-1", "1.123456+1", "1.123456789012garbage", "1.1234567890123x"}) {
            for (int precision : new int[] {6, 12}) {
                assertTrinoExceptionThrownBy(assertions.expression("CAST('%s' AS INTERVAL SECOND(2,%s))".formatted(value, precision))::evaluate)
                        .hasErrorCode(INVALID_LITERAL);
                assertTrinoExceptionThrownBy(assertions.expression("INTERVAL '%s' SECOND(2,%s)".formatted(value, precision))::evaluate)
                        .hasErrorCode(INVALID_LITERAL);
            }
        }
        assertThat(assertions.expression("CAST('1.1234567890129' AS INTERVAL SECOND(2,12)) = INTERVAL '1.123456789013' SECOND(2,12)"))
                .isEqualTo(true);
    }

    @Test
    public void testRangeWithLongIntervalOffset()
    {
        for (int precision : new int[] {6, 9, 12}) {
            String unit = "0." + "0".repeat(precision - 1) + "1";
            String next = "0." + "0".repeat(precision - 1) + "2";
            assertThat(assertions.query(
                    """
                    SELECT sum(x) OVER (ORDER BY ts RANGE BETWEEN
                        INTERVAL '%s' SECOND(1,%s) PRECEDING AND CURRENT ROW)
                    FROM (VALUES
                        (1, TIMESTAMP '2024-01-01 00:00:0%s'),
                        (2, TIMESTAMP '2024-01-01 00:00:0%s')) t(x, ts)
                    """.formatted(unit, precision, unit, next)))
                    .matches("VALUES BIGINT '1', BIGINT '3'");
        }
    }

    @Test
    public void testTimeZoneOffsetPreservesPrecisionUntilValidation()
    {
        for (String datetime : new String[] {
                "TIMESTAMP '2024-01-01 00:00:00 UTC'",
                "TIMESTAMP '2024-01-01 00:00:00.123456789012 UTC'",
                "TIME '00:00:00 +00:00'",
                "TIME '00:00:00.123456789012 +00:00'",
        }) {
            for (String sign : new String[] {"", "-"}) {
                assertThat(assertions.expression("CAST(%s AT TIME ZONE INTERVAL '%s3600' SECOND(4,12) AS VARCHAR) = CAST(%s AT TIME ZONE INTERVAL '%s1' HOUR AS VARCHAR)".formatted(datetime, sign, datetime, sign)))
                        .isEqualTo(true);
                for (String offset : new String[] {"3600.000000000001", "0.000000000001", "3600.000001", "3601"}) {
                    assertTrinoExceptionThrownBy(assertions.expression("%s AT TIME ZONE INTERVAL '%s%s' SECOND(4,12)".formatted(datetime, sign, offset))::evaluate)
                            .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                            .hasMessage("Invalid time zone offset interval: interval contains seconds");
                }
            }
        }
    }

    @Test
    public void testParseDurationPhysicalRange()
    {
        assertThat(assertions.expression("parse_duration('106751991d') = INTERVAL '106751991' DAY(9)"))
                .isEqualTo(true);
        assertThat(assertions.expression("parse_duration('9223372036854s') = INTERVAL '9223372036854' SECOND(13,6)"))
                .isEqualTo(true);
        for (String value : new String[] {"106751992d", "9223372036855s"}) {
            assertTrinoExceptionThrownBy(assertions.expression("parse_duration('%s')".formatted(value))::evaluate)
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        }
        for (String value : new String[] {"-106751991d", "-106751992d", "-9223372036854s", "-9223372036855s"}) {
            assertTrinoExceptionThrownBy(assertions.expression("parse_duration('%s')".formatted(value))::evaluate)
                    .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        }
    }
}
