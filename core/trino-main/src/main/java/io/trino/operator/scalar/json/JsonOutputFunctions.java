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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.json.Json;
import io.trino.json.JsonItemEncoding;
import io.trino.json.JsonItemEncoding.TypeTag;
import io.trino.json.JsonItems;
import io.trino.spi.TrinoException;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.sql.tree.JsonQuery.EmptyOrErrorBehavior.EMPTY_ARRAY;
import static io.trino.sql.tree.JsonQuery.EmptyOrErrorBehavior.EMPTY_OBJECT;
import static io.trino.sql.tree.JsonQuery.EmptyOrErrorBehavior.ERROR;
import static io.trino.sql.tree.JsonQuery.EmptyOrErrorBehavior.NULL;
import static java.util.Objects.requireNonNull;

/**
 * Format JSON as binary or character string, using given encoding.
 * <p>
 * These functions are used to format the output of JSON_QUERY function.
 * In case of error during JSON formatting, the error handling
 * strategy of the enclosing JSON_QUERY function is applied.
 * <p>
 * Additionally, the options KEEP / OMIT QUOTES [ON SCALAR STRING]
 * are respected when formatting the output.
 */
public final class JsonOutputFunctions
{
    public static final String JSON_TO_JSON_OUTPUT = "$json_to_json_output";
    public static final String JSON_TO_VARCHAR = "$json_to_varchar";
    public static final String JSON_TO_VARBINARY = "$json_to_varbinary";
    public static final String JSON_TO_VARBINARY_UTF8 = "$json_to_varbinary_utf8";
    public static final String JSON_TO_VARBINARY_UTF16 = "$json_to_varbinary_utf16";
    public static final String JSON_TO_VARBINARY_UTF32 = "$json_to_varbinary_utf32";

    private static final JsonMapper MAPPER = new JsonMapper();
    private static final EncodingSpecificConstants UTF_8 = new EncodingSpecificConstants(
            JsonEncoding.UTF8,
            StandardCharsets.UTF_8,
            Slices.copiedBuffer(new ArrayNode(JsonNodeFactory.instance).asText(), StandardCharsets.UTF_8),
            Slices.copiedBuffer(new ObjectNode(JsonNodeFactory.instance).asText(), StandardCharsets.UTF_8));
    private static final EncodingSpecificConstants UTF_16 = new EncodingSpecificConstants(
            JsonEncoding.UTF16_LE,
            StandardCharsets.UTF_16LE,
            Slices.copiedBuffer(new ArrayNode(JsonNodeFactory.instance).asText(), StandardCharsets.UTF_16LE),
            Slices.copiedBuffer(new ObjectNode(JsonNodeFactory.instance).asText(), StandardCharsets.UTF_16LE));
    private static final EncodingSpecificConstants UTF_32 = new EncodingSpecificConstants(
            JsonEncoding.UTF32_LE,
            StandardCharsets.UTF_32LE,
            Slices.copiedBuffer(new ArrayNode(JsonNodeFactory.instance).asText(), StandardCharsets.UTF_32LE),
            Slices.copiedBuffer(new ObjectNode(JsonNodeFactory.instance).asText(), StandardCharsets.UTF_32LE));

    private JsonOutputFunctions() {}

    @SqlNullable
    @ScalarFunction(value = JSON_TO_JSON_OUTPUT, hidden = true)
    @SqlType(StandardTypes.JSON)
    public static Json jsonToJson(@SqlType(StandardTypes.JSON) Json jsonExpression, @SqlType(StandardTypes.TINYINT) long errorBehavior, @SqlType(StandardTypes.BOOLEAN) boolean omitQuotes)
    {
        // The omitQuotes parameter is part of the JSON_QUERY output-conversion contract but is
        // always false here: SQL:2023 §6.35 SR 3 forbids OMIT QUOTES on a JSON-typed return,
        // and ExpressionAnalyzer.analyzeJsonQueryExpression rejects it at analysis time.
        // Producing bare scalar text in this code path would manufacture an invalid JSON value.
        Slice text = serialize(jsonExpression, UTF_8, errorBehavior, false);
        return text == null ? null : JsonItems.fromText(text);
    }

    @SqlNullable
    @ScalarFunction(value = JSON_TO_VARCHAR, hidden = true)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice jsonToVarchar(@SqlType(StandardTypes.JSON) Json jsonExpression, @SqlType(StandardTypes.TINYINT) long errorBehavior, @SqlType(StandardTypes.BOOLEAN) boolean omitQuotes)
    {
        return serialize(jsonExpression, UTF_8, errorBehavior, omitQuotes);
    }

    @SqlNullable
    @ScalarFunction(value = JSON_TO_VARBINARY, hidden = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice jsonToVarbinary(@SqlType(StandardTypes.JSON) Json jsonExpression, @SqlType(StandardTypes.TINYINT) long errorBehavior, @SqlType(StandardTypes.BOOLEAN) boolean omitQuotes)
    {
        return jsonToVarbinaryUtf8(jsonExpression, errorBehavior, omitQuotes);
    }

    @SqlNullable
    @ScalarFunction(value = JSON_TO_VARBINARY_UTF8, hidden = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice jsonToVarbinaryUtf8(@SqlType(StandardTypes.JSON) Json jsonExpression, @SqlType(StandardTypes.TINYINT) long errorBehavior, @SqlType(StandardTypes.BOOLEAN) boolean omitQuotes)
    {
        return serialize(jsonExpression, UTF_8, errorBehavior, omitQuotes);
    }

    @SqlNullable
    @ScalarFunction(value = JSON_TO_VARBINARY_UTF16, hidden = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice jsonToVarbinaryUtf16(@SqlType(StandardTypes.JSON) Json jsonExpression, @SqlType(StandardTypes.TINYINT) long errorBehavior, @SqlType(StandardTypes.BOOLEAN) boolean omitQuotes)
    {
        return serialize(jsonExpression, UTF_16, errorBehavior, omitQuotes);
    }

    @SqlNullable
    @ScalarFunction(value = JSON_TO_VARBINARY_UTF32, hidden = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice jsonToVarbinaryUtf32(@SqlType(StandardTypes.JSON) Json jsonExpression, @SqlType(StandardTypes.TINYINT) long errorBehavior, @SqlType(StandardTypes.BOOLEAN) boolean omitQuotes)
    {
        return serialize(jsonExpression, UTF_32, errorBehavior, omitQuotes);
    }

    private static Slice serialize(Json json, EncodingSpecificConstants constants, long errorBehavior, boolean omitQuotes)
    {
        // A JSON_ERROR input carries the failure forward from a failOnError=false input
        // function call (e.g. JSON_SERIALIZE / JSON_VALUE / JSON_QUERY under NULL ON ERROR
        // when the input couldn't be parsed). The dispatch applies uniformly to all four
        // ON ERROR ordinals (NULL/ERROR/EMPTY_ARRAY/EMPTY_OBJECT) so the caller's clause
        // governs the outcome regardless of which leg produced the sentinel.
        if (json.isError()) {
            return onError(constants, errorBehavior, new JsonOutputConversionException("JSON value cannot be serialized"));
        }

        if (omitQuotes && json.isScalar() && json.scalarType() == TypeTag.VARCHAR) {
            // unquoted-scalar-string output: emit the raw text without JSON-escaping the wrapping quotes
            String text = ((Slice) json.materializeScalar().getObjectValue()).toStringUtf8();
            return Slices.copiedBuffer(text, constants.charset);
        }

        DynamicSliceOutput output = new DynamicSliceOutput(64);
        try (JsonGenerator generator = MAPPER.createGenerator(output, constants.jsonEncoding)) {
            // Write the typed encoding directly so duplicate object keys round-trip
            // (SQL:2023 §9.42 'WITHOUT UNIQUE KEYS'). Going through ObjectNode would
            // collapse duplicates because ObjectNode is keyed by field name.
            JsonItemEncoding.writeJson(json.backingSlice(), json.viewOffset(), generator);
        }
        catch (JsonProcessingException e) {
            return onError(constants, errorBehavior, new JsonOutputConversionException(e));
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
        return output.slice();
    }

    private static Slice onError(EncodingSpecificConstants constants, long errorBehavior, JsonOutputConversionException cause)
    {
        if (errorBehavior == NULL.ordinal()) {
            return null;
        }
        if (errorBehavior == ERROR.ordinal()) {
            throw cause;
        }
        if (errorBehavior == EMPTY_ARRAY.ordinal()) {
            return constants.emptyArray;
        }
        if (errorBehavior == EMPTY_OBJECT.ordinal()) {
            return constants.emptyObject;
        }
        throw new IllegalStateException("unexpected behavior");
    }

    private static class EncodingSpecificConstants
    {
        private final JsonEncoding jsonEncoding;
        private final Charset charset;
        private final Slice emptyArray;
        private final Slice emptyObject;

        public EncodingSpecificConstants(JsonEncoding jsonEncoding, Charset charset, Slice emptyArray, Slice emptyObject)
        {
            this.jsonEncoding = requireNonNull(jsonEncoding, "jsonEncoding is null");
            this.charset = requireNonNull(charset, "charset is null");
            this.emptyArray = requireNonNull(emptyArray, "emptyArray is null");
            this.emptyObject = requireNonNull(emptyObject, "emptyObject is null");
        }
    }
}
