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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.airlift.slice.Slice;
import io.trino.json.Json;
import io.trino.json.JsonItemBuilder;
import io.trino.json.JsonItems;
import io.trino.spi.TrinoException;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
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
 * (a [Json] value with [Json.Kind#ERROR]) is returned, so that the enclosing
 * function can handle the error accordingly to its error handling strategy
 * (e.g. return a default value).
 * <p>
 * A duplicate key in a JSON object does not cause error.
 * The resulting object has one entry with that key, chosen arbitrarily.
 * This behavior fulfills the 'WITHOUT UNIQUE KEYS' option. (SQL standard p. 692)
 */
public final class JsonInputFunctions
{
    public static final String JSON_TO_JSON = "$json_to_json";
    public static final String VARCHAR_TO_JSON = "$varchar_to_json";
    public static final String VARBINARY_TO_JSON = "$varbinary_to_json";
    public static final String VARBINARY_UTF8_TO_JSON = "$varbinary_utf8_to_json";
    public static final String VARBINARY_UTF16_TO_JSON = "$varbinary_utf16_to_json";
    public static final String VARBINARY_UTF32_TO_JSON = "$varbinary_utf32_to_json";

    private JsonInputFunctions() {}

    /// Identity input for a JSON-typed expression. Surfaces the existing [Json] value as the
    /// path engine's input without re-parsing; only re-checks the FORMAT-JSON `failOnError`
    /// contract by raising if a [Json] error sentinel arrives under `ERROR ON ERROR`.
    @ScalarFunction(value = JSON_TO_JSON, hidden = true)
    @SqlType(StandardTypes.JSON)
    public static Json jsonToJson(@SqlType(StandardTypes.JSON) Json inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        if (failOnError && inputExpression.isError()) {
            throw new JsonInputConversionException(new IllegalArgumentException("malformed JSON"));
        }
        return inputExpression;
    }

    @ScalarFunction(value = VARCHAR_TO_JSON, hidden = true)
    @SqlType(StandardTypes.JSON)
    public static Json varcharToJson(@SqlType(StandardTypes.VARCHAR) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        // Pass the raw UTF-8 bytes directly to the byte-mode Jackson parser instead
        // of wrapping in InputStreamReader (which would force decode-to-chars).
        return toJson(inputExpression, failOnError);
    }

    @ScalarFunction(value = VARBINARY_TO_JSON, hidden = true)
    @SqlType(StandardTypes.JSON)
    public static Json varbinaryToJson(@SqlType(StandardTypes.VARBINARY) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        return varbinaryUtf8ToJson(inputExpression, failOnError);
    }

    @ScalarFunction(value = VARBINARY_UTF8_TO_JSON, hidden = true)
    @SqlType(StandardTypes.JSON)
    public static Json varbinaryUtf8ToJson(@SqlType(StandardTypes.VARBINARY) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        // Wrap as a UTF-8 InputStreamReader so the parser rejects non-UTF-8 bytes
        // (the contract of `$varbinary_utf8_to_json` is strict UTF-8). Going through
        // the byte-mode parser directly would silently auto-detect UTF-16/32 from a
        // BOM and accept inputs that should be rejected.
        Reader reader = new InputStreamReader(inputExpression.getInput(), UTF_8);
        return toJson(reader, failOnError);
    }

    @ScalarFunction(value = VARBINARY_UTF16_TO_JSON, hidden = true)
    @SqlType(StandardTypes.JSON)
    public static Json varbinaryUtf16ToJson(@SqlType(StandardTypes.VARBINARY) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        Reader reader = new InputStreamReader(inputExpression.getInput(), UTF_16LE);
        return toJson(reader, failOnError);
    }

    @ScalarFunction(value = VARBINARY_UTF32_TO_JSON, hidden = true)
    @SqlType(StandardTypes.JSON)
    public static Json varbinaryUtf32ToJson(@SqlType(StandardTypes.VARBINARY) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        Reader reader = new InputStreamReader(inputExpression.getInput(), UTF_32LE);
        return toJson(reader, failOnError);
    }

    private static Json toJson(Slice utf8Bytes, boolean failOnError)
    {
        try {
            return JsonItems.parseToTree(utf8Bytes);
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(INVALID_FUNCTION_ARGUMENT.toErrorCode())) {
                if (failOnError) {
                    throw new JsonInputConversionException(e);
                }
                return JsonItemBuilder.JSON_ERROR;
            }
            throw e;
        }
    }

    private static Json toJson(Reader reader, boolean failOnError)
    {
        // Parse straight to the tree form so duplicate object keys survive as separate
        // members. Reading into a Jackson tree first would route through ObjectNode, which
        // is a Map and so collapses duplicate keys to the last value.
        try {
            return JsonItems.parseToTree(reader);
        }
        catch (JsonProcessingException e) {
            if (failOnError) {
                throw new JsonInputConversionException(e);
            }
            return JsonItemBuilder.JSON_ERROR;
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
        catch (TrinoException e) {
            // parseToTree reports malformed input as TrinoException — surface it
            // through this function's documented error-handling contract.
            if (e.getErrorCode().equals(INVALID_FUNCTION_ARGUMENT.toErrorCode())) {
                if (failOnError) {
                    throw new JsonInputConversionException(e);
                }
                return JsonItemBuilder.JSON_ERROR;
            }
            throw e;
        }
    }
}
