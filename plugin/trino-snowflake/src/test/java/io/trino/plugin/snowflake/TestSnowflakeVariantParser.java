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
package io.trino.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.trino.plugin.snowflake.SnowflakeVariantParser.parseVariant;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSnowflakeVariantParser
{
    @Test
    void testScalars()
    {
        assertParsed("null", null);
        assertParsed("true", true);
        assertParsed("false", false);
        assertParsed("\"abc\"", "abc");
        assertParsed("\"\"", "");
        assertParsed("\"a\\\"b\\\\c\\/d\\n\\t\\u00e9\"", "a\"b\\c/d\n\té");
        assertParsed("\"2024-01-02 03:04:05.678\"", "2024-01-02 03:04:05.678");
    }

    @Test
    void testNumbers()
    {
        assertParsed("0", 0);
        assertParsed("-42", -42);
        assertParsed("2147483648", 2147483648L);
        assertParsed("-9223372036854775808", Long.MIN_VALUE);
        assertParsed("9223372036854775808", new BigDecimal("9223372036854775808"));
        assertParsed("99999999999999999999999999999999999999", new BigDecimal("99999999999999999999999999999999999999"));
        assertParsed("1.50", new BigDecimal("1.50"));
        assertParsed("-0.001", new BigDecimal("-0.001"));
        // Snowflake prints doubles with an exponent
        assertParsed("1.820120000000000e+03", 1820.12);
        assertParsed("-1.500000000000000e-01", -0.15);
        assertParsed("1E2", 100.0);
        assertParsed("NaN", Double.NaN);
        assertParsed("Infinity", Double.POSITIVE_INFINITY);
        assertParsed("-Infinity", Double.NEGATIVE_INFINITY);
        // more digits than a variant decimal can hold
        assertParsed("1234567890.12345678901234567890123456789", 1234567890.12345678901234567890123456789);
    }

    @Test
    void testUndefined()
    {
        assertParsed("undefined", null);
        assertParsed("[1,2,null,4,undefined,6,undefined,8]", Arrays.asList(1, 2, null, 4, null, 6, null, 8));
        Map<String, Object> expected = new HashMap<>();
        expected.put("a", 1);
        expected.put("b", null);
        expected.put("c", null);
        assertParsed("{\"a\":1,\"b\":null,\"c\":undefined}", expected);
    }

    @Test
    void testArraysAndObjects()
    {
        assertParsed("[]", Arrays.asList());
        assertParsed("{}", ImmutableMap.of());
        assertParsed(
                """
                {
                  "coordinates": [
                    1.820120000000000e+03,
                    8.905599999999999e+02
                  ],
                  "type": "Point"
                }
                """,
                ImmutableMap.of("coordinates", Arrays.asList(1820.12, 890.5599999999999), "type", "Point"));
        assertParsed(
                "{\"a\": [1, \"x\", {\"b\": [true, false]}], \"c\": {\"d\": {}}, \"e\": []}",
                ImmutableMap.of(
                        "a", Arrays.asList(1, "x", ImmutableMap.of("b", Arrays.asList(true, false))),
                        "c", ImmutableMap.of("d", ImmutableMap.of()),
                        "e", Arrays.asList()));
    }

    @Test
    void testInvalid()
    {
        assertInvalid("", "Unexpected end of input at position 0");
        assertInvalid("  ", "Unexpected end of input at position 2");
        assertInvalid("[1,]", "Invalid value at position 3");
        assertInvalid("[1,,3]", "Invalid value at position 3");
        assertInvalid("[1 2]", "Expected ',' or ']' at position 3");
        assertInvalid("{\"a\" 1}", "Expected ':' at position 5");
        assertInvalid("{a: 1}", "Expected field name at position 1");
        assertInvalid("{\"a\": 1, \"a\": 2}", "Duplicate field name: a at position 15");
        assertInvalid("\"abc", "Unterminated string at position 4");
        assertInvalid("\"\\x\"", "Invalid escape character: x at position 3");
        assertInvalid("\"\\u12\"", "Invalid unicode escape at position 3");
        assertInvalid("tru", "Invalid value at position 0");
        assertInvalid("1 2", "Unexpected trailing data at position 2");
        assertInvalid("-", "Invalid value at position 0");
    }

    private static void assertParsed(String text, Object expected)
    {
        assertThat(parseVariant(text).toObject()).isEqualTo(expected);
    }

    private static void assertInvalid(String text, String message)
    {
        assertThatThrownBy(() -> parseVariant(text))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message + " in Snowflake VARIANT value");
    }
}
