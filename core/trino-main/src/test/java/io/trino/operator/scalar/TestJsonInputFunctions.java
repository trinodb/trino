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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.Charset;

import static com.google.common.io.BaseEncoding.base16;
import static io.trino.json.JsonInputErrorNode.JSON_ERROR;
import static io.trino.spi.StandardErrorCode.JSON_INPUT_CONVERSION_ERROR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.Json2016Type.JSON_2016;
import static java.nio.charset.StandardCharsets.UTF_16BE;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestJsonInputFunctions
{
    private static final String INPUT = "{\"key1\" : 1e0, \"key2\" : true, \"key3\" : null}";
    private static final JsonNode JSON_OBJECT = new ObjectNode(
            JsonNodeFactory.instance,
            ImmutableMap.of("key1", DoubleNode.valueOf(1e0), "key2", BooleanNode.TRUE, "key3", NullNode.instance));
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
        assertThat(assertions.expression("\"$varchar_to_json\"('[]', true)"))
                .hasType(JSON_2016)
                .isEqualTo(new ArrayNode(JsonNodeFactory.instance));

        assertThat(assertions.expression("\"$varchar_to_json\"('" + INPUT + "', true)"))
                .hasType(JSON_2016)
                .isEqualTo(JSON_OBJECT);

        // with unsuppressed input conversion error
        assertTrinoExceptionThrownBy(assertions.expression("\"$varchar_to_json\"('" + ERROR_INPUT + "', true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // with input conversion error suppressed and converted to JSON_ERROR
        assertThat(assertions.expression("\"$varchar_to_json\"('" + ERROR_INPUT + "', false)"))
                .hasType(JSON_2016)
                .isEqualTo(JSON_ERROR);
    }

    @Test
    public void testVarbinaryUtf8ToJson()
    {
        assertThat(assertions.expression("\"$varbinary_to_json\"(" + toVarbinary(INPUT, UTF_8) + ", true)"))
                .hasType(JSON_2016)
                .isEqualTo(JSON_OBJECT);

        assertThat(assertions.expression("\"$varbinary_utf8_to_json\"(" + toVarbinary(INPUT, UTF_8) + ", true)"))
                .hasType(JSON_2016)
                .isEqualTo(JSON_OBJECT);

        // wrong input encoding

        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf8_to_json\"(" + toVarbinary(INPUT, UTF_16LE) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // wrong input encoding; conversion error suppressed and converted to JSON_ERROR
        assertThat(assertions.expression("\"$varbinary_utf8_to_json\"(" + toVarbinary(INPUT, UTF_16LE) + ", false)"))
                .hasType(JSON_2016)
                .isEqualTo(JSON_ERROR);

        // correct encoding, incorrect input

        // with unsuppressed input conversion error
        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf8_to_json\"(" + toVarbinary(ERROR_INPUT, UTF_8) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // with input conversion error suppressed and converted to JSON_ERROR
        assertThat(assertions.expression("\"$varbinary_utf8_to_json\"(" + toVarbinary(ERROR_INPUT, UTF_8) + ", false)"))
                .hasType(JSON_2016)
                .isEqualTo(JSON_ERROR);
    }

    @Test
    public void testVarbinaryUtf16ToJson()
    {
        assertThat(assertions.expression("\"$varbinary_utf16_to_json\"(" + toVarbinary(INPUT, UTF_16LE) + ", true)"))
                .hasType(JSON_2016)
                .isEqualTo(JSON_OBJECT);

        // wrong input encoding
        String varbinaryLiteral = toVarbinary(INPUT, UTF_16BE);

        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf16_to_json\"(" + varbinaryLiteral + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf16_to_json\"(" + toVarbinary(INPUT, UTF_8) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // wrong input encoding; conversion error suppressed and converted to JSON_ERROR
        assertThat(assertions.expression("\"$varbinary_utf16_to_json\"(" + toVarbinary(INPUT, UTF_8) + ", false)"))
                .hasType(JSON_2016)
                .isEqualTo(JSON_ERROR);

        // correct encoding, incorrect input

        // with unsuppressed input conversion error
        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf16_to_json\"(" + toVarbinary(ERROR_INPUT, UTF_16LE) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // with input conversion error suppressed and converted to JSON_ERROR
        assertThat(assertions.expression("\"$varbinary_utf16_to_json\"(" + toVarbinary(ERROR_INPUT, UTF_16LE) + ", false)"))
                .hasType(JSON_2016)
                .isEqualTo(JSON_ERROR);
    }

    @Test
    public void testVarbinaryUtf32ToJson()
    {
        assertThat(assertions.expression("\"$varbinary_utf32_to_json\"(" + toVarbinary(INPUT, Charset.forName("UTF-32LE")) + ", true)"))
                .hasType(JSON_2016)
                .isEqualTo(JSON_OBJECT);

        // wrong input encoding

        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf32_to_json\"(" + toVarbinary(INPUT, Charset.forName("UTF-32BE")) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf32_to_json\"(" + toVarbinary(INPUT, UTF_8) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // wrong input encoding; conversion error suppressed and converted to JSON_ERROR
        assertThat(assertions.expression("\"$varbinary_utf32_to_json\"(" + toVarbinary(INPUT, UTF_8) + ", false)"))
                .hasType(JSON_2016)
                .isEqualTo(JSON_ERROR);

        // correct encoding, incorrect input

        // with unsuppressed input conversion error
        assertTrinoExceptionThrownBy(assertions.expression("\"$varbinary_utf32_to_json\"(" + toVarbinary(ERROR_INPUT, Charset.forName("UTF-32LE")) + ", true)")::evaluate)
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // with input conversion error suppressed and converted to JSON_ERROR
        assertThat(assertions.expression("\"$varbinary_utf32_to_json\"(" + toVarbinary(ERROR_INPUT, Charset.forName("UTF-32LE")) + ", false)"))
                .hasType(JSON_2016)
                .isEqualTo(JSON_ERROR);
    }

    @Test
    public void testNullInput()
    {
        assertThat(assertions.expression("\"$varchar_to_json\"(null, true)"))
                .isNull(JSON_2016);
    }

    @Test
    public void testDuplicateObjectKeys()
    {
        // A duplicate key does not cause error. The resulting object has one member with that key, chosen arbitrarily from the input entries.
        // According to the SQL standard, this behavior is a correct implementation of the 'WITHOUT UNIQUE KEYS' option.
        assertThat(assertions.expression("\"$varchar_to_json\"('{\"key\" : 1, \"key\" : 2}', true)"))
                .hasType(JSON_2016)
                .isIn(
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key", IntNode.valueOf(1))),
                        new ObjectNode(JsonNodeFactory.instance, ImmutableMap.of("key", IntNode.valueOf(2))));
    }

    private static String toVarbinary(String value, Charset encoding)
    {
        return "X'" + base16().encode(value.getBytes(encoding)) + "'";
    }
}
