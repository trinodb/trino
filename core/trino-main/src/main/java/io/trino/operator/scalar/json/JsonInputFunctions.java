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
package io.trino.operator.scalar.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.NumberType;
import com.fasterxml.jackson.core.JsonParser.NumberTypeFP;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.util.JsonParserDelegate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.MissingNode;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import static io.trino.jsonpath.JsonInputErrorNode.JSON_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_32LE;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Read string input as JSON.
 * <p>
 * These functions are used by JSON_EXISTS, JSON_VALUE and JSON_QUERY functions
 * for parsing the JSON input arguments and applicable JSON path parameters.
 * <p>
 * If the error handling strategy of the enclosing JSON function is ERROR ON ERROR,
 * these input functions throw exception in case of parse error.
 * Otherwise, the parse error is suppressed, and a marker value JSON_ERROR
 * is returned, so that the enclosing function can handle the error accordingly
 * to its error handling strategy (e.g. return a default value).
 * <p>
 * A duplicate key in a JSON object does not cause error.
 * The resulting object has one entry with that key, chosen arbitrarily.
 * This behavior fulfills the 'WITHOUT UNIQUE KEYS' option. (SQL standard p. 692)
 */
public final class JsonInputFunctions
{
    public static final String VARCHAR_TO_JSON = "$varchar_to_json";
    public static final String VARBINARY_TO_JSON = "$varbinary_to_json";
    public static final String VARBINARY_UTF8_TO_JSON = "$varbinary_utf8_to_json";
    public static final String VARBINARY_UTF16_TO_JSON = "$varbinary_utf16_to_json";
    public static final String VARBINARY_UTF32_TO_JSON = "$varbinary_utf32_to_json";

    // trailing zeros are part of the scale the literal declared, so they are preserved
    private static final JsonMapper MAPPER = JsonMapper.builder()
            .nodeFactory(JsonNodeFactory.withExactBigDecimals(true))
            .build();

    private JsonInputFunctions() {}

    @ScalarFunction(value = VARCHAR_TO_JSON, hidden = true)
    @SqlType(StandardTypes.JSON_2016)
    public static JsonNode varcharToJson(@SqlType(StandardTypes.VARCHAR) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        Reader reader = new InputStreamReader(inputExpression.getInput(), UTF_8);
        return toJson(reader, failOnError);
    }

    @ScalarFunction(value = VARBINARY_TO_JSON, hidden = true)
    @SqlType(StandardTypes.JSON_2016)
    public static JsonNode varbinaryToJson(@SqlType(StandardTypes.VARBINARY) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        return varbinaryUtf8ToJson(inputExpression, failOnError);
    }

    @ScalarFunction(value = VARBINARY_UTF8_TO_JSON, hidden = true)
    @SqlType(StandardTypes.JSON_2016)
    public static JsonNode varbinaryUtf8ToJson(@SqlType(StandardTypes.VARBINARY) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        Reader reader = new InputStreamReader(inputExpression.getInput(), UTF_8);
        return toJson(reader, failOnError);
    }

    @ScalarFunction(value = VARBINARY_UTF16_TO_JSON, hidden = true)
    @SqlType(StandardTypes.JSON_2016)
    public static JsonNode varbinaryUtf16ToJson(@SqlType(StandardTypes.VARBINARY) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        Reader reader = new InputStreamReader(inputExpression.getInput(), UTF_16LE);
        return toJson(reader, failOnError);
    }

    @ScalarFunction(value = VARBINARY_UTF32_TO_JSON, hidden = true)
    @SqlType(StandardTypes.JSON_2016)
    public static JsonNode varbinaryUtf32ToJson(@SqlType(StandardTypes.VARBINARY) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        Reader reader = new InputStreamReader(inputExpression.getInput(), UTF_32LE);
        return toJson(reader, failOnError);
    }

    private static JsonNode toJson(Reader reader, boolean failOnError)
    {
        try {
            return readTree(reader);
        }
        catch (JsonProcessingException e) {
            if (failOnError) {
                throw new JsonInputConversionException(e);
            }
            return JSON_ERROR;
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    /**
     * Reads JSON text into a tree.
     * <p>
     * SQL:2023 9.42 GR 4 defines a JSON number as the value of the {@code <signed numeric literal>}
     * whose characters are identical to it, so the form of the literal selects the type: a literal
     * written with an exponent is approximate, and a literal written without one is exact. Jackson
     * reads floating point tokens through {@code double} by default, which loses the digits of an
     * exact literal, so such tokens are reported as {@link NumberType#BIG_DECIMAL} instead.
     */
    public static JsonNode readTree(Reader reader)
            throws IOException
    {
        try (JsonParser parser = new ExactDecimalParser(MAPPER.createParser(reader))) {
            JsonNode node = MAPPER.readTree(parser);
            // reading from a parser yields null for empty input, where reading from a reader yields a missing node
            return node == null ? MissingNode.getInstance() : node;
        }
    }

    public static JsonNode readTree(String json)
            throws IOException
    {
        return readTree(Reader.of(json));
    }

    private static class ExactDecimalParser
            extends JsonParserDelegate
    {
        public ExactDecimalParser(JsonParser delegate)
        {
            super(delegate);
        }

        @Override
        public NumberType getNumberType()
                throws IOException
        {
            if (isExact()) {
                return NumberType.BIG_DECIMAL;
            }
            return super.getNumberType();
        }

        @Override
        public NumberTypeFP getNumberTypeFP()
                throws IOException
        {
            if (isExact()) {
                return NumberTypeFP.BIG_DECIMAL;
            }
            return super.getNumberTypeFP();
        }

        private boolean isExact()
                throws IOException
        {
            if (currentToken() != JsonToken.VALUE_NUMBER_FLOAT) {
                return false;
            }
            String text = getText();
            return text.indexOf('e') < 0 && text.indexOf('E') < 0;
        }
    }
}
