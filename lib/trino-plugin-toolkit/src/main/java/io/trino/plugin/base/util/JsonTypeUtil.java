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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.util.JsonUtils.jsonFactoryBuilder;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class JsonTypeUtil
{
    private static final JsonFactory JSON_FACTORY = jsonFactoryBuilder().disable(CANONICALIZE_FIELD_NAMES).build();
    private static final ObjectMapper SORTED_MAPPER = new ObjectMapperProvider().get().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);

    private JsonTypeUtil() {}

    // TODO: this should be available from the engine
    public static Slice jsonParse(Slice slice)
    {
        // cast(json_parse(x) AS t)` will be optimized into `$internal$json_string_to_array/map/row_cast` in ExpressionOptimizer
        // If you make changes to this function (e.g. use parse JSON string into some internal representation),
        // make sure `$internal$json_string_to_array/map/row_cast` is changed accordingly.
        try (JsonParser parser = createJsonParser(JSON_FACTORY, slice)) {
            SliceOutput output = new DynamicSliceOutput(slice.length());
            SORTED_MAPPER.writeValue((OutputStream) output, SORTED_MAPPER.readValue(parser, Object.class));
            // At this point, the end of input should be reached. nextToken() has three possible results:
            // - null, if the end of the input was reached
            // - token, if a correct JSON token is found (e.g. '{', 'null', '1')
            // - exception, if there are characters which do not form a valid JSON token (e.g. 'abc')
            checkState(parser.nextToken() == null, "Found characters after the expected end of input");
            return output.slice();
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Cannot convert value to JSON: '%s'", slice.toStringUtf8()), e);
        }
    }

    public static Slice toJsonValue(Object value)
            throws IOException
    {
        return Slices.wrappedBuffer(SORTED_MAPPER.writeValueAsBytes(value));
    }

    private static JsonParser createJsonParser(JsonFactory factory, Slice json)
            throws IOException
    {
        // Jackson tries to detect the character encoding automatically when
        // using InputStream, so we pass an InputStreamReader instead.
        return factory.createParser(new InputStreamReader(json.getInput(), UTF_8));
    }
}
