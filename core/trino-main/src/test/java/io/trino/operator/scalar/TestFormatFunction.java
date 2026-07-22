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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.trino.sql.query.QueryAssertions;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Arrays;

import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestFormatFunction
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testFormat()
    {
        assertThat(format("%s%%", "123"))
                .isEqualTo("123%");

        assertThat(format("%.4f", "pi()"))
                .isEqualTo("3.1416");

        assertThat(format("%.5f", "pi()"))
                .isEqualTo("3.14159");

        assertThat(format("%03d", "8"))
                .isEqualTo("008");

        assertThat(format("%-7s,%7s", "'hello'", "'world'"))
                .isEqualTo("hello  ,  world");

        assertThat(format("%b %B %b", "true", "false", "null"))
                .isEqualTo("true FALSE false");

        assertThat(format("%s %s %s", "true", "false", "null"))
                .isEqualTo("true false null");

        assertThat(format("%S %S %S", "true", "false", "null"))
                .isEqualTo("TRUE FALSE NULL");

        assertThat(format("%4$s %3$s %2$s %1$s %4$s %3$s %2$s %1$s", "'a'", "'b'", "'c'", "'d'"))
                .isEqualTo("d c b a d c b a");

        assertThat(format("%s %s %<s %<s", "'a'", "'b'", "'c'", "'d'"))
                .isEqualTo("a b b b");

        assertThat(format("%2$s %s %<s %s", "'a'", "'b'", "'c'", "'d'"))
                .isEqualTo("b a a b");

        assertThat(format("%2$s %3$s %1$s", "'a'", "'b'", "'c'", "'d'"))
                .isEqualTo("b c a");

        assertThat(format("%s %s", "2", "3", "4"))
                .isEqualTo("2 3");

        assertThat(format("%d", "tinyint '123'"))
                .isEqualTo("123");

        assertThat(format("%d", "smallint '32123'"))
                .isEqualTo("32123");

        assertThat(format("%d", "1234567890"))
                .isEqualTo("1234567890");

        assertThat(format("%d", "1234567890123"))
                .isEqualTo("1234567890123");

        assertThat(format("%,.2f", "1234567.89"))
                .isEqualTo("1,234,567.89");

        assertThat(format("%1$s %1$f %1$.2f", "decimal '9.12345678'"))
                .isEqualTo("9.12345678 9.123457 9.12");

        assertThat(format("%1$d %1$x %1$X %1$o", "1234"))
                .isEqualTo("1234 4d2 4D2 2322");

        assertThat(format("%s", "ipaddress '192.168.88.123'"))
                .isEqualTo("192.168.88.123");

        assertThat(format("%s", "ipaddress '2001:db8:0:0:1:0:0:1'"))
                .isEqualTo("2001:db8::1:0:0:1");

        assertThat(format("%s", "json '[123,\"abc\",true]'"))
                .isEqualTo("[123,\"abc\",true]");

        assertThat(format("%1$s %1$tF %1$tY-%1$tm-%1$td", "date '2001-08-22'"))
                .isEqualTo("2001-08-22 2001-08-22 2001-08-22");

        assertThat(format("%1$tA, %1$tB %1$te, %1$tY", "date '2006-07-04'"))
                .isEqualTo("Tuesday, July 4, 2006");

        assertThat(format("%1$s %1$tT %1$tr", "time '16:17:13'"))
                .isEqualTo("16:17:13 16:17:13 04:17:13 PM");

        assertThat(format("%1$s %1$tF %1$tT", "timestamp '1969-07-20 16:17:00'"))
                .isEqualTo("1969-07-20T16:17 1969-07-20 16:17:00");

        assertThat(format("%1$s %1$tF %1$tT", "timestamp '1969-07-20 16:17:03'"))
                .isEqualTo("1969-07-20T16:17:03 1969-07-20 16:17:03");

        assertThat(format("%1$s %1$tc", "cast('1969-07-20 16:17:00 America/New_York' AS timestamp with time zone)"))
                .isEqualTo("1969-07-20T16:17-04:00[America/New_York] Sun Jul 20 16:17:00 EDT 1969");

        assertThat(format("%1$s %1$tc", "cast('1969-07-20 20:17:00 UTC' AS timestamp with time zone)"))
                .isEqualTo("1969-07-20T20:17Z[UTC] Sun Jul 20 20:17:00 UTC 1969");

        assertThat(format("%s", "cast('16:17:13 -05:00' AS time with time zone)"))
                .isEqualTo("16:17:13.000-05:00");

        assertThat(format("%s", "cast('test' AS char(5))"))
                .isEqualTo("test ");

        assertThat(format("%s", "cast(row('hello', 'world') AS row(greeting varchar, planet varchar))"))
                .isEqualTo("{\"greeting\": \"hello\", \"planet\": \"world\"}");

        assertThat(format("%s", "row('hello', 'world')"))
                .isEqualTo("[\"hello\", \"world\"]");

        assertThat(format("%s", "cast(row('hello', array['world']) AS row(greeting varchar, planet array(varchar)))"))
                .isEqualTo("{\"greeting\": \"hello\", \"planet\": [\"world\"]}");

        assertThat(format("%s", "cast(row('hello', 1337) AS row(greeting varchar, planet integer))"))
                .isEqualTo("{\"greeting\": \"hello\", \"planet\": 1337}");

        assertThat(format("%s", "cast(row('hello', from_base64('d29ybGQ=')) AS row(greeting varchar, planet varbinary))"))
                .isEqualTo("{\"greeting\": \"hello\", \"planet\": \"d29ybGQ=\"}");

        assertThat(format("%s", "ARRAY['hello', 'world']"))
                .isEqualTo("[\"hello\", \"world\"]");

        assertThat(format("%s", "ARRAY['hel\"l\\o\nworld']"))
                .isEqualTo("[\"hel\\\"l\\\\o\\nworld\"]");

        assertThat(format("%s", "ARRAY[1, 2, 3]"))
                .isEqualTo("[1, 2, 3]");

        assertThat(format("%s", "ARRAY[TRUE, FALSE]"))
                .isEqualTo("[true, false]");

        assertThat(format("%s", "from_base64('d29ybGQ=')"))
                .isEqualTo("d29ybGQ=");

        assertThat(format("%s", "map(ARRAY['greeting', 'planet'], ARRAY['hello', 'world'])"))
                .isEqualTo("{\"greeting\": \"hello\", \"planet\": \"world\"}");

        assertThat(format("%s", "map(ARRAY['greeting', 'planet'], ARRAY[1, 2])"))
                .isEqualTo("{\"greeting\": 1, \"planet\": 2}");

        assertThat(format("%s", "ARRAY[null, 'world']"))
                .isEqualTo("[null, \"world\"]");

        assertThat(format("%s", "cast(row(null, 'world') AS row(greeting varchar, planet varchar))"))
                .isEqualTo("{\"greeting\": null, \"planet\": \"world\"}");

        assertThat(format("%s", "map(ARRAY['greeting'], ARRAY[null])"))
                .isEqualTo("{\"greeting\": null}");

        assertThat(format("%s", "ARRAY[cast('hello' AS char(5))]"))
                .isEqualTo("[\"hello\"]");

        assertThat(format("%s", "cast(row(cast('hi' AS char(3))) AS row(greeting char(3)))"))
                .isEqualTo("{\"greeting\": \"hi \"}");

        assertThat(format("%s", "ARRAY[ARRAY['a', 'b'], ARRAY['c']]"))
                .isEqualTo("[[\"a\", \"b\"], [\"c\"]]");

        assertThat(format("%s", "map(ARRAY['nums'], ARRAY[ARRAY[1, 2]])"))
                .isEqualTo("{\"nums\": [1, 2]}");

        assertThat(format("%s", "map(ARRAY[1, 2], ARRAY['hello', 'world'])"))
                .isEqualTo("{1: \"hello\", 2: \"world\"}");

        assertThat(format("%s", "ARRAY[uuid '03780fd9-76cf-4366-b720-0cfc6b957e8f']"))
                .isEqualTo("[\"03780fd9-76cf-4366-b720-0cfc6b957e8f\"]");

        assertThat(format("%s", "row(uuid '03780fd9-76cf-4366-b720-0cfc6b957e8f')"))
                .isEqualTo("[\"03780fd9-76cf-4366-b720-0cfc6b957e8f\"]");

        assertThat(format("%s", "cast(row(uuid '03780fd9-76cf-4366-b720-0cfc6b957e8f') AS row(id uuid))"))
                .isEqualTo("{\"id\": \"03780fd9-76cf-4366-b720-0cfc6b957e8f\"}");

        assertThat(format("%s", "map(ARRAY['id'], ARRAY[uuid '03780fd9-76cf-4366-b720-0cfc6b957e8f'])"))
                .isEqualTo("{\"id\": \"03780fd9-76cf-4366-b720-0cfc6b957e8f\"}");

        assertTrinoExceptionThrownBy(format("%.4d", "8")::evaluate)
                .hasMessage("Invalid format string: %.4d (IllegalFormatPrecision: 4)");

        assertTrinoExceptionThrownBy(format("%-02d", "8")::evaluate)
                .hasMessage("Invalid format string: %-02d (IllegalFormatFlags: Flags = '-0')");

        assertTrinoExceptionThrownBy(format("%--2d", "8")::evaluate)
                .hasMessage("Invalid format string: %--2d (DuplicateFormatFlags: Flags = '-')");

        assertTrinoExceptionThrownBy(format("%+s", "8")::evaluate)
                .hasMessage("Invalid format string: %+s (FormatFlagsConversionMismatch: Conversion = s, Flags = +)");

        assertTrinoExceptionThrownBy(format("%-s", "8")::evaluate)
                .hasMessage("Invalid format string: %-s (MissingFormatWidth: %-s)");

        assertTrinoExceptionThrownBy(format("%5n", "8")::evaluate)
                .hasMessage("Invalid format string: %5n (IllegalFormatWidth: 5)");

        assertTrinoExceptionThrownBy(format("%s %d", "8")::evaluate)
                .hasMessage("Invalid format string: %s %d (MissingFormatArgument: Format specifier '%d')");

        assertTrinoExceptionThrownBy(format("%d", "decimal '8'")::evaluate)
                .hasMessage("Invalid format string: %d (IllegalFormatConversion: d != java.math.BigDecimal)");

        assertTrinoExceptionThrownBy(format("%tT", "current_time")::evaluate)
                .hasMessage("Invalid format string: %tT (IllegalFormatConversion: T != java.lang.String)");

        assertTrinoExceptionThrownBy(assertions.function("format", "5", "8")::evaluate)
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:17: Type of first argument to format() must be VARCHAR (actual: integer)");
    }

    private QueryAssertions.ExpressionAssertProvider format(String format, @Language("SQL") String... arguments)
    {
        return assertions.function(
                "format",
                ImmutableList.<String>builder()
                        .add("'%s'".formatted(format))
                        .addAll(Arrays.asList(arguments))
                        .build());
    }
}
