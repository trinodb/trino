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
package io.trino.plugin.base.util;

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestJsonTypeUtil
{
    @Test
    void testJsonParseNull()
    {
        assertThat(jsonParse(utf8Slice("null")).toStringUtf8())
                .isEqualTo("null");
    }

    @Test
    void testJsonParseBoolean()
    {
        assertThat(jsonParse(utf8Slice("true")).toStringUtf8())
                .isEqualTo("true");
        assertThat(jsonParse(utf8Slice("false")).toStringUtf8())
                .isEqualTo("false");
    }

    @Test
    void testJsonParseString()
    {
        assertThat(jsonParse(utf8Slice("\"hello\"")).toStringUtf8())
                .isEqualTo("\"hello\"");
        assertThat(jsonParse(utf8Slice("\"with\\nescapes\"")).toStringUtf8())
                .isEqualTo("\"with\\nescapes\"");
    }

    @Test
    void testJsonParseInteger()
    {
        assertThat(jsonParse(utf8Slice("123")).toStringUtf8())
                .isEqualTo("123");
        assertThat(jsonParse(utf8Slice("-456")).toStringUtf8())
                .isEqualTo("-456");
        assertThat(jsonParse(utf8Slice("0")).toStringUtf8())
                .isEqualTo("0");
    }

    @Test
    void testJsonParseDecimal()
    {
        assertThat(jsonParse(utf8Slice("3.14")).toStringUtf8())
                .isEqualTo("3.14");
        assertThat(jsonParse(utf8Slice("-2.5")).toStringUtf8())
                .isEqualTo("-2.5");
        assertThat(jsonParse(utf8Slice("1.0")).toStringUtf8())
                .isEqualTo("1.0");
        assertThat(jsonParse(utf8Slice("100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e-106")).toStringUtf8())
                .isEqualTo("10.0");
        assertThat(jsonParse(utf8Slice("100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e-107")).toStringUtf8())
                .isEqualTo("1.0");
        assertThat(jsonParse(utf8Slice("100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e-108")).toStringUtf8())
                .isEqualTo("0.1");
    }

    @Test
    void testJsonParseLargeNumber()
    {
        assertThat(jsonParse(utf8Slice("12345678901234567890123456789012345678")).toStringUtf8())
                .isEqualTo("12345678901234567890123456789012345678");
        assertThat(jsonParse(utf8Slice("123456789012345678901234567890.12345678")).toStringUtf8())
                // TODO precision loss! Numbers are converted through floating-point instead of being preserved as strings
                .isEqualTo("1.2345678901234568E29");
    }

    @Test
    void testJsonParseNumberWithLeadingZeros()
    {
        assertThatThrownBy(() -> jsonParse(utf8Slice("007")))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot convert value to JSON: '007'");
        assertThatThrownBy(() -> jsonParse(utf8Slice("007.0")))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot convert value to JSON: '007.0'");
    }

    @Test
    void testJsonParseNumberWithTrailingZeros()
    {
        assertThat(jsonParse(utf8Slice("7000000100000000")).toStringUtf8())
                .isEqualTo("7000000100000000");
        assertThat(jsonParse(utf8Slice("7010.00")).toStringUtf8())
                .isEqualTo("7010.0");
        assertThat(jsonParse(utf8Slice("7000000100000000.000000000000")).toStringUtf8())
                .isEqualTo("7.0000001E15");
        assertThat(jsonParse(utf8Slice("7010.030")).toStringUtf8())
                .isEqualTo("7010.03");
        assertThat(jsonParse(utf8Slice("7000000100000000.0000030000000")).toStringUtf8())
                .isEqualTo("7.0000001E15");
    }

    @Test
    void testJsonParseArray()
    {
        assertThat(jsonParse(utf8Slice("[]")).toStringUtf8())
                .isEqualTo("[]");
        assertThat(jsonParse(utf8Slice("[1,2,3]")).toStringUtf8())
                .isEqualTo("[1,2,3]");
        assertThat(jsonParse(utf8Slice("[\"a\",\"b\",\"c\"]")).toStringUtf8())
                .isEqualTo("[\"a\",\"b\",\"c\"]");
        assertThat(jsonParse(utf8Slice("[null,true,false]")).toStringUtf8())
                .isEqualTo("[null,true,false]");
    }

    @Test
    void testJsonParseObject()
    {
        assertThat(jsonParse(utf8Slice("{}")).toStringUtf8())
                .isEqualTo("{}");
        // Note: jsonParse sorts object keys (normalization)
        assertThat(jsonParse(utf8Slice("{\"b\":2,\"a\":1}")).toStringUtf8())
                .isEqualTo("{\"a\":1,\"b\":2}");
        assertThat(jsonParse(utf8Slice("{\"name\":\"value\"}")).toStringUtf8())
                .isEqualTo("{\"name\":\"value\"}");
    }

    @Test
    void testJsonParseNested()
    {
        assertThat(jsonParse(utf8Slice("{\"arr\":[1,2,3],\"obj\":{\"x\":10}}")).toStringUtf8())
                .isEqualTo("{\"arr\":[1,2,3],\"obj\":{\"x\":10}}");

        assertThat(jsonParse(utf8Slice("[{\"b\":2,\"c\":3,\"a\":1}]")).toStringUtf8())
                .isEqualTo("[{\"a\":1,\"b\":2,\"c\":3}]");
    }

    @Test
    void testJsonParseInvalidJson()
    {
        assertThatThrownBy(() -> jsonParse(utf8Slice("invalid")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot convert value to JSON")
                .extracting(e -> ((TrinoException) e).getErrorCode().getType())
                .isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode().getType());

        assertThatThrownBy(() -> jsonParse(utf8Slice("{incomplete")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot convert value to JSON");

        assertThatThrownBy(() -> jsonParse(utf8Slice("[1,2,]")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot convert value to JSON");
    }

    @Test
    void testJsonParseTrailingCharacters()
    {
        assertThatThrownBy(() -> jsonParse(utf8Slice("123 extra")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot convert value to JSON");

        assertThatThrownBy(() -> jsonParse(utf8Slice("{}abc")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot convert value to JSON");
    }
}
