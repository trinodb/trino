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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.NumberType;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.airlift.slice.Slice;
import io.trino.json.Json;
import io.trino.json.JsonItemEncoding.TypeTag;
import io.trino.json.JsonItems;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.type.JsonPathType;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_FALSE;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_FLOAT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static com.fasterxml.jackson.core.JsonToken.VALUE_TRUE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonUtils.jsonFactoryBuilder;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.util.JsonUtil.createJsonParser;
import static io.trino.util.JsonUtil.truncateIfNecessaryForErrorMessage;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class JsonFunctions
{
    private static final JsonMapper JSON_MAPPER = new JsonMapper(jsonFactoryBuilder()
            .disable(CANONICALIZE_FIELD_NAMES)
            .build());

    private static final JsonMapper MAPPING_JSON_MAPPER = new JsonMapper(MappingJsonFactory.builder()
            .disable(CANONICALIZE_FIELD_NAMES)
            .build());

    private JsonFunctions() {}

    // fallible
    @ScalarOperator(OperatorType.CAST)
    @SqlType(JsonPathType.NAME)
    @LiteralParameters("x")
    public static JsonPath castVarcharToJsonPath(@SqlType("varchar(x)") Slice pattern)
    {
        return new JsonPath(pattern.toStringUtf8());
    }

    // fallible
    @ScalarOperator(OperatorType.CAST)
    @LiteralParameters("x")
    @SqlType(JsonPathType.NAME)
    public static JsonPath castCharToJsonPath(@LiteralParameter("x") Long charLength, @SqlType("char(x)") Slice pattern)
    {
        return new JsonPath(padSpaces(pattern, charLength.intValue()).toStringUtf8());
    }

    @ScalarFunction("is_json_scalar")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean varcharIsJsonScalar(@SqlType("varchar(x)") Slice json)
    {
        // Operate on raw text directly: parsing first would report malformed input as the
        // generic "invalid JSON: ..." from JsonItems.fromText, rather than this function's
        // own "Invalid JSON value" diagnostic.
        return isJsonScalarText(json);
    }

    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isJsonScalar(@SqlType(StandardTypes.JSON) Json json)
    {
        return !json.isArray() && !json.isObject();
    }

    private static boolean isJsonScalarText(Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_MAPPER, json)) {
            JsonToken nextToken = parser.nextToken();
            if (nextToken == null) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid JSON value: " + truncateIfNecessaryForErrorMessage(json));
            }
            if (nextToken == START_ARRAY || nextToken == START_OBJECT) {
                parser.skipChildren();
                if (parser.nextToken() != null) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid JSON value: " + truncateIfNecessaryForErrorMessage(json));
                }
                return false;
            }
            if (parser.nextToken() != null) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid JSON value: " + truncateIfNecessaryForErrorMessage(json));
            }
            return true;
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid JSON value: " + truncateIfNecessaryForErrorMessage(json));
        }
    }

    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice jsonFormat(@SqlType(StandardTypes.JSON) Json json)
    {
        return JsonItems.toText(json);
    }

    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.JSON)
    public static Json jsonParse(@SqlType("varchar(x)") Slice slice)
    {
        try {
            return JsonItems.fromText(slice);
        }
        catch (TrinoException e) {
            // Keep json_parse's own error surface rather than leaking the parser's detail.
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Cannot convert value to JSON: '%s'", slice.toStringUtf8()), e);
        }
    }

    @SqlNullable
    @ScalarFunction("json_array_length")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static Long varcharJsonArrayLength(@SqlType("varchar(x)") Slice json)
    {
        return jsonArrayLengthText(json);
    }

    @SqlNullable
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static Long jsonArrayLength(@SqlType(StandardTypes.JSON) Json jsonValue)
    {
        // Typed Json carries structure end-to-end: read arraySize() directly rather than
        // round-tripping through toText + Jackson.
        if (!jsonValue.isArray()) {
            return null;
        }
        return (long) jsonValue.arraySize();
    }

    private static Long jsonArrayLengthText(Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_MAPPER, json)) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }
            long length = 0;
            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return length;
                }
                parser.skipChildren();

                length++;
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @SqlNullable
    @ScalarFunction("json_array_contains")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharJsonArrayContains(@SqlType("varchar(x)") Slice json, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return jsonArrayContainsText(json, value);
    }

    @SqlNullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean jsonArrayContains(@SqlType(StandardTypes.JSON) Json jsonValue, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        // A raw-text value streams and returns on the first match; going through isArray() /
        // anyArrayElement would parse the whole array up front. Typed values already carry
        // structure, so traverse them in place.
        if (jsonValue.isRawText()) {
            return jsonArrayContainsText(jsonValue.rawText(), value);
        }
        if (!jsonValue.isArray()) {
            return null;
        }
        return jsonValue.anyArrayElement(item -> item.isScalar() && item.scalarType() == TypeTag.BOOLEAN
                && item.materializeScalar().getBooleanValue() == value);
    }

    private static Boolean jsonArrayContainsText(Slice json, boolean value)
    {
        try (JsonParser parser = createJsonParser(JSON_MAPPER, json)) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return false;
                }
                parser.skipChildren();

                if (((token == VALUE_TRUE) && value) ||
                        ((token == VALUE_FALSE) && !value)) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @SqlNullable
    @ScalarFunction("json_array_contains")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharJsonArrayContains(@SqlType("varchar(x)") Slice json, @SqlType(StandardTypes.BIGINT) long value)
    {
        return jsonArrayContainsText(json, value);
    }

    @SqlNullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean jsonArrayContains(@SqlType(StandardTypes.JSON) Json jsonValue, @SqlType(StandardTypes.BIGINT) long value)
    {
        if (jsonValue.isRawText()) {
            return jsonArrayContainsText(jsonValue.rawText(), value);
        }
        if (!jsonValue.isArray()) {
            return null;
        }
        return jsonValue.anyArrayElement(item -> {
            if (!item.isScalar()) {
                return false;
            }
            // Match the text-path semantics: only long-fitting integer scalars (BIGINT /
            // INTEGER / SMALLINT / TINYINT) are eligible; NUMBER scalars carrying values too
            // large for long are skipped just as the text parser skips BIG_INTEGER tokens.
            TypeTag tag = item.scalarType();
            return (tag == TypeTag.BIGINT || tag == TypeTag.INTEGER || tag == TypeTag.SMALLINT || tag == TypeTag.TINYINT)
                    && item.materializeScalar().getLongValue() == value;
        });
    }

    private static Boolean jsonArrayContainsText(Slice json, long value)
    {
        try (JsonParser parser = createJsonParser(JSON_MAPPER, json)) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return false;
                }
                parser.skipChildren();

                if ((token == VALUE_NUMBER_INT) &&
                        ((parser.getNumberType() == NumberType.INT) || (parser.getNumberType() == NumberType.LONG)) &&
                        (parser.getLongValue() == value)) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @SqlNullable
    @ScalarFunction("json_array_contains")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharJsonArrayContains(@SqlType("varchar(x)") Slice json, @SqlType(StandardTypes.DOUBLE) double value)
    {
        return jsonArrayContainsText(json, value);
    }

    @SqlNullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean jsonArrayContains(@SqlType(StandardTypes.JSON) Json jsonValue, @SqlType(StandardTypes.DOUBLE) double value)
    {
        // TODO Direct structural walk for double matching needs a unified "treat any
        // numeric scalar as a candidate" path covering DOUBLE / REAL / DECIMAL / NUMBER
        // scalars; today the canonical text + VALUE_NUMBER_FLOAT shape is the simplest
        // way to keep the same semantics across all of those.
        return jsonArrayContainsText(JsonItems.toText(jsonValue), value);
    }

    private static Boolean jsonArrayContainsText(Slice json, double value)
    {
        if (!Double.isFinite(value)) {
            return false;
        }

        try (JsonParser parser = createJsonParser(JSON_MAPPER, json)) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return false;
                }
                parser.skipChildren();

                // noinspection FloatingPointEquality
                if ((token == VALUE_NUMBER_FLOAT) && (parser.getDoubleValue() == value) &&
                        Double.isFinite(parser.getDoubleValue())) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @SqlNullable
    @ScalarFunction("json_array_contains")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharJsonArrayContains(@SqlType("varchar(x)") Slice json, @SqlType("varchar(y)") Slice value)
    {
        return jsonArrayContainsText(json, value);
    }

    @SqlNullable
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean jsonArrayContains(@SqlType(StandardTypes.JSON) Json jsonValue, @SqlType("varchar(x)") Slice value)
    {
        if (jsonValue.isRawText()) {
            return jsonArrayContainsText(jsonValue.rawText(), value);
        }
        if (!jsonValue.isArray()) {
            return null;
        }
        return jsonValue.anyArrayElement(item -> item.isScalar() && item.scalarType() == TypeTag.VARCHAR
                && value.equals(item.materializeScalar().getObjectValue()));
    }

    private static Boolean jsonArrayContainsText(Slice json, Slice value)
    {
        String valueString = value.toStringUtf8();

        try (JsonParser parser = createJsonParser(JSON_MAPPER, json)) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return false;
                }
                parser.skipChildren();

                if (token == VALUE_STRING && valueString.equals(parser.getValueAsString())) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @SqlNullable
    @ScalarFunction("json_array_get")
    @LiteralParameters("x")
    @SqlType(StandardTypes.JSON)
    public static Json varcharJsonArrayGet(@SqlType("varchar(x)") Slice json, @SqlType(StandardTypes.BIGINT) long index)
    {
        // jsonArrayGetText emits valid JSON text; wrap raw to skip the fromText
        // round-trip on the way to writeObject.
        Slice result = jsonArrayGetText(json, index);
        return result == null ? null : Json.unchecked(result);
    }

    @SqlNullable
    @ScalarFunction
    @SqlType(StandardTypes.JSON)
    public static Json jsonArrayGet(@SqlType(StandardTypes.JSON) Json jsonValue, @SqlType(StandardTypes.BIGINT) long index)
    {
        // this value cannot be converted to positive number
        if (index == Long.MIN_VALUE) {
            return null;
        }
        // A raw-text value parses only up to the requested element; going structural would
        // materialize the whole array first.
        if (jsonValue.isRawText()) {
            Slice element = jsonArrayGetText(jsonValue.rawText(), index);
            return element == null ? null : Json.unchecked(element);
        }
        if (!jsonValue.isArray()) {
            return null;
        }
        int size = jsonValue.arraySize();
        long resolvedIndex = index < 0 ? size + index : index;
        if (resolvedIndex < 0 || resolvedIndex >= size) {
            return null;
        }
        // The index is within the array bounds, so read it directly: indexed encodings resolve
        // in O(1), and a linear one stops at the element rather than walking to the end.
        Json element = jsonValue.arrayElement(toIntExact(resolvedIndex));
        // Match the text-path semantics: a JSON null element is returned as SQL null
        // rather than as a JSON null value.
        if (element.kind() == Json.Kind.NULL) {
            return null;
        }
        return element;
    }

    private static Slice jsonArrayGetText(Slice json, long index)
    {
        // this value cannot be converted to positive number
        if (index == Long.MIN_VALUE) {
            return null;
        }

        try (JsonParser parser = createJsonParser(MAPPING_JSON_MAPPER, json)) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            List<String> tokens = null;
            if (index < 0) {
                tokens = new LinkedList<>();
            }

            long count = 0;
            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    if (tokens != null && count >= index * -1) {
                        return utf8Slice(tokens.get(0));
                    }

                    return null;
                }

                String arrayElement;
                if (token == START_OBJECT || token == START_ARRAY) {
                    arrayElement = parser.readValueAsTree().toString();
                }
                else if (token == JsonToken.VALUE_NULL) {
                    arrayElement = null;
                }
                else {
                    // Read the scalar via the tree API so we get a properly-quoted JSON
                    // representation ("jhfa" rather than the bare jhfa from
                    // parser.getValueAsString). The downstream JsonItems.fromText
                    // requires valid JSON; bare text would fail validation.
                    arrayElement = parser.readValueAsTree().toString();
                }

                if (count == index) {
                    return arrayElement == null ? null : utf8Slice(arrayElement);
                }

                if (tokens != null) {
                    tokens.add(arrayElement);

                    if (count >= index * -1) {
                        tokens.remove(0);
                    }
                }

                count++;
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @ScalarFunction("json_extract_scalar")
    @SqlNullable
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice varcharJsonExtractScalar(@SqlType("varchar(x)") Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getScalarExtractor());
    }

    @ScalarFunction
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice jsonExtractScalar(@SqlType(StandardTypes.JSON) Json jsonValue, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(JsonItems.toText(jsonValue), jsonPath.getScalarExtractor());
    }

    @ScalarFunction("json_extract")
    @LiteralParameters("x")
    @SqlNullable
    @SqlType(StandardTypes.JSON)
    public static Json varcharJsonExtract(@SqlType("varchar(x)") Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        // JsonExtract emits valid JSON text; wrap raw to skip the fromText / typed-encoding
        // round-trip on the way to writeObject. Consumers that need structural access
        // pay the parse on demand.
        Slice result = JsonExtract.extract(json, jsonPath.getObjectExtractor());
        return result == null ? null : Json.unchecked(result);
    }

    @ScalarFunction
    @SqlNullable
    @SqlType(StandardTypes.JSON)
    public static Json jsonExtract(@SqlType(StandardTypes.JSON) Json jsonValue, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        // JsonExtract emits valid JSON text; wrap raw to skip the fromText / typed-encoding
        // round-trip on the way to writeObject — symmetric with varcharJsonExtract.
        Slice result = JsonExtract.extract(JsonItems.toText(jsonValue), jsonPath.getObjectExtractor());
        return result == null ? null : Json.unchecked(result);
    }

    @ScalarFunction("json_size")
    @LiteralParameters("x")
    @SqlNullable
    @SqlType(StandardTypes.BIGINT)
    public static Long varcharJsonSize(@SqlType("varchar(x)") Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getSizeExtractor());
    }

    @ScalarFunction
    @SqlNullable
    @SqlType(StandardTypes.BIGINT)
    public static Long jsonSize(@SqlType(StandardTypes.JSON) Json jsonValue, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(JsonItems.toText(jsonValue), jsonPath.getSizeExtractor());
    }
}
