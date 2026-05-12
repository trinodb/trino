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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.airlift.slice.Slices;
import io.trino.json.Json;
import io.trino.json.JsonItems;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static com.google.common.io.BaseEncoding.base16;
import static io.trino.spi.StandardErrorCode.JSON_INPUT_CONVERSION_ERROR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.JsonType.JSON;
import static java.nio.charset.StandardCharsets.UTF_16BE;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonInputFunctions
{
    private static final String INPUT = "{\"key1\" : 1, \"key2\" : true, \"key3\" : null}";
    // Integer JSON literals parse as BIGINT-typed values; using a plain `1` here matches
    // the round-trip used by assertJsonValue (text → fromText → typed encoding) and avoids
    // the DECIMAL/INTEGER mismatch that decimal-form literals (`1e0`) would introduce.
    private static final Json JSON_OBJECT = JsonItems.fromText(Slices.utf8Slice("{\"key1\":1,\"key2\":true,\"key3\":null}"));
    private static final String ERROR_INPUT = "[...";

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
    public void testVarcharToJson()
    {
        assertJsonValue("\"$varchar_to_json\"('[]', true)", JsonItems.fromJsonNode(new ArrayNode(JsonNodeFactory.instance)));
        assertJsonValue("\"$varchar_to_json\"('" + INPUT + "', true)", JSON_OBJECT);

        // with unsuppressed input conversion error
        assertTrinoExceptionThrownBy(assertions.expression("\"$varchar_to_json\"('" + ERROR_INPUT + "', true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // with input conversion error suppressed and converted to JSON_ERROR
        assertJsonError("\"$varchar_to_json\"('" + ERROR_INPUT + "', false)");
    }

    @Test
    public void testVarbinaryUtf8ToJson()
    {
        assertJsonValue("\"$varbinary_to_json\"(" + toVarbinary(INPUT, UTF_8) + ", true)", JSON_OBJECT);
        assertJsonValue("\"$varbinary_utf8_to_json\"(" + toVarbinary(INPUT, UTF_8) + ", true)", JSON_OBJECT);

        // wrong input encoding

        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf8_to_json\"(" + toVarbinary(INPUT, UTF_16LE) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // wrong input encoding; conversion error suppressed and converted to JSON_ERROR
        assertJsonError("\"$varbinary_utf8_to_json\"(" + toVarbinary(INPUT, UTF_16LE) + ", false)");

        // correct encoding, incorrect input

        // with unsuppressed input conversion error
        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf8_to_json\"(" + toVarbinary(ERROR_INPUT, UTF_8) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // with input conversion error suppressed and converted to JSON_ERROR
        assertJsonError("\"$varbinary_utf8_to_json\"(" + toVarbinary(ERROR_INPUT, UTF_8) + ", false)");
    }

    @Test
    public void testVarbinaryUtf16ToJson()
    {
        assertJsonValue("\"$varbinary_utf16_to_json\"(" + toVarbinary(INPUT, UTF_16LE) + ", true)", JSON_OBJECT);

        // wrong input encoding
        String varbinaryLiteral = toVarbinary(INPUT, UTF_16BE);

        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf16_to_json\"(" + varbinaryLiteral + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf16_to_json\"(" + toVarbinary(INPUT, UTF_8) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // wrong input encoding; conversion error suppressed and converted to JSON_ERROR
        assertJsonError("\"$varbinary_utf16_to_json\"(" + toVarbinary(INPUT, UTF_8) + ", false)");

        // correct encoding, incorrect input

        // with unsuppressed input conversion error
        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf16_to_json\"(" + toVarbinary(ERROR_INPUT, UTF_16LE) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // with input conversion error suppressed and converted to JSON_ERROR
        assertJsonError("\"$varbinary_utf16_to_json\"(" + toVarbinary(ERROR_INPUT, UTF_16LE) + ", false)");
    }

    @Test
    public void testVarbinaryUtf32ToJson()
    {
        assertJsonValue("\"$varbinary_utf32_to_json\"(" + toVarbinary(INPUT, StandardCharsets.UTF_32LE) + ", true)", JSON_OBJECT);

        // wrong input encoding

        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf32_to_json\"(" + toVarbinary(INPUT, StandardCharsets.UTF_32BE) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf32_to_json\"(" + toVarbinary(INPUT, UTF_8) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // wrong input encoding; conversion error suppressed and converted to JSON_ERROR
        assertJsonError("\"$varbinary_utf32_to_json\"(" + toVarbinary(INPUT, UTF_8) + ", false)");

        // correct encoding, incorrect input

        // with unsuppressed input conversion error
        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf32_to_json\"(" + toVarbinary(ERROR_INPUT, StandardCharsets.UTF_32LE) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // with input conversion error suppressed and converted to JSON_ERROR
        assertJsonError("\"$varbinary_utf32_to_json\"(" + toVarbinary(ERROR_INPUT, StandardCharsets.UTF_32LE) + ", false)");
    }

    @Test
    public void testNullInput()
    {
        assertThat(assertions.expression("\"$varchar_to_json\"(null, true)"))
                .isNull(JSON);
    }

    @Test
    public void testDuplicateObjectKeys()
    {
        // A duplicate key does not cause error. The resulting object preserves all members
        // with that key — JsonItems.fromText streams via Jackson's token API directly into
        // a JsonItemBuilder, bypassing ObjectNode's collapse-to-Map behavior. This matches
        // SQL:2023 §9.42 'WITHOUT UNIQUE KEYS'.
        assertJsonValue(
                "\"$varchar_to_json\"('{\"key\" : 1, \"key\" : 2}', true)",
                JsonItems.fromText(Slices.utf8Slice("{\"key\":1,\"key\":2}")));
    }

    /// Compares the function's typed-encoding output to {@code expected} via a text round-trip.
    /// JsonType's getObjectValue stringifies on read, so we re-parse the actual text back to a
    /// Json before comparing — preserves number-precision and duplicate-key fidelity.
    private void assertJsonValue(String expression, Json expected)
    {
        assertThat(assertions.expression(expression))
                .hasType(JSON)
                .satisfies(actual -> assertThat(JsonItems.fromText(Slices.utf8Slice((String) actual))).isEqualTo(expected));
    }

    /// Asserts the function produces the JSON_ERROR sentinel under {@code failOnError = false}.
    /// The sentinel can't be rendered as JSON text, so JsonType's getObjectValue propagates an
    /// IllegalArgumentException at materialization — we capture that as the witness.
    private void assertJsonError(String expression)
    {
        // Trino's test client unwraps worker-side exceptions into io.trino.execution.Failure;
        // assert on the message text rather than the class to stay decoupled from that wrapping.
        assertThatThrownBy(assertions.expression(expression)::evaluate)
                .hasRootCauseMessage("JSON_ERROR cannot be rendered as JSON text");
    }

    private static String toVarbinary(String value, Charset encoding)
    {
        return "X'" + base16().encode(value.getBytes(encoding)) + "'";
    }
}
