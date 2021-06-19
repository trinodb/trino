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

import org.testng.annotations.Test;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestFormatFunction
        extends AbstractTestFunctions
{
    @Test
    public void testFormat()
    {
        assertFormat("format('%s%%', 123)", "123%");
        assertFormat("format('%.4f', pi())", "3.1416");
        assertFormat("format('%.5f', pi())", "3.14159");
        assertFormat("format('%03d', 8)", "008");
        assertFormat("format('%-7s,%7s', 'hello', 'world')", "hello  ,  world");
        assertFormat("format('%b %B %b', true, false, null)", "true FALSE false");
        assertFormat("format('%s %s %s', true, false, null)", "true false null");
        assertFormat("format('%S %S %S', true, false, null)", "TRUE FALSE NULL");
        assertFormat("format('%4$s %3$s %2$s %1$s %4$s %3$s %2$s %1$s', 'a', 'b', 'c', 'd')", "d c b a d c b a");
        assertFormat("format('%s %s %<s %<s', 'a', 'b', 'c', 'd')", "a b b b");
        assertFormat("format('%2$s %s %<s %s', 'a', 'b', 'c', 'd')", "b a a b");
        assertFormat("format('%2$s %3$s %1$s', 'a', 'b', 'c', 'd')", "b c a");
        assertFormat("format('%s %s', 2, 3, 4)", "2 3");
        assertFormat("format('%d', tinyint '123')", "123");
        assertFormat("format('%d', smallint '32123')", "32123");
        assertFormat("format('%d', 1234567890)", "1234567890");
        assertFormat("format('%d', 1234567890123)", "1234567890123");
        assertFormat("format('%,.2f', 1234567.89)", "1,234,567.89");
        assertFormat("format('%1$s %1$f %1$.2f', decimal '9.12345678')", "9.12345678 9.123457 9.12");
        assertFormat("format('%1$d %1$x %1$X %1$o', 1234)", "1234 4d2 4D2 2322");
        assertFormat("format('%s', ipaddress '192.168.88.123')", "192.168.88.123");
        assertFormat("format('%s', ipaddress '2001:db8:0:0:1:0:0:1')", "2001:db8::1:0:0:1");
        assertFormat("format('%s', json '[123,\"abc\",true]')", "[123,\"abc\",true]");
        assertFormat("format('%1$s %1$tF %1$tY-%1$tm-%1$td', date '2001-08-22')", "2001-08-22 2001-08-22 2001-08-22");
        assertFormat("format('%1$tA, %1$tB %1$te, %1$tY', date '2006-07-04')", "Tuesday, July 4, 2006");
        assertFormat("format('%1$s %1$tT %1$tr', time '16:17:13')", "16:17:13 16:17:13 04:17:13 PM");
        assertFormat("format('%1$s %1$tF %1$tT', timestamp '1969-07-20 16:17:00')", "1969-07-20T16:17 1969-07-20 16:17:00");
        assertFormat("format('%1$s %1$tF %1$tT', timestamp '1969-07-20 16:17:03')", "1969-07-20T16:17:03 1969-07-20 16:17:03");
        assertFormat("format('%1$s %1$tc', cast('1969-07-20 16:17:00 America/New_York' AS timestamp with time zone))", "1969-07-20T16:17-04:00[America/New_York] Sun Jul 20 16:17:00 EDT 1969");
        assertFormat("format('%1$s %1$tc', cast('1969-07-20 20:17:00 UTC' AS timestamp with time zone))", "1969-07-20T20:17Z[UTC] Sun Jul 20 20:17:00 UTC 1969");
        assertFormat("format('%s', cast('16:17:13 -05:00' AS time with time zone))", "16:17:13.000-05:00");
        assertFormat("format('%s', cast('test' AS char(5)))", "test ");

        assertInvalidFunction("format('%.4d', 8)", "Invalid format string: %.4d (IllegalFormatPrecision: 4)");
        assertInvalidFunction("format('%-02d', 8)", "Invalid format string: %-02d (IllegalFormatFlags: Flags = '-0')");
        assertInvalidFunction("format('%--2d', 8)", "Invalid format string: %--2d (DuplicateFormatFlags: Flags = '-')");
        assertInvalidFunction("format('%+s', 8)", "Invalid format string: %+s (FormatFlagsConversionMismatch: Conversion = s, Flags = +)");
        assertInvalidFunction("format('%-s', 8)", "Invalid format string: %-s (MissingFormatWidth: %-s)");
        assertInvalidFunction("format('%5n', 8)", "Invalid format string: %5n (IllegalFormatWidth: 5)");
        assertInvalidFunction("format('%s %d', 8)", "Invalid format string: %s %d (MissingFormatArgument: Format specifier '%d')");
        assertInvalidFunction("format('%d', decimal '8')", "Invalid format string: %d (IllegalFormatConversion: d != java.math.BigDecimal)");
        assertInvalidFunction("format('%tT', current_time)", "Invalid format string: %tT (IllegalFormatConversion: T != java.lang.String)");
        assertInvalidFunction("format('%s', array[8])", NOT_SUPPORTED, "line 1:14: Type not supported for formatting: array(integer)");
        assertInvalidFunction("format(5, 8)", TYPE_MISMATCH, "line 1:8: Type of first argument to format() must be VARCHAR (actual: integer)");
    }

    private void assertFormat(String projection, String expected)
    {
        assertFunction(projection, VARCHAR, expected);
    }
}
