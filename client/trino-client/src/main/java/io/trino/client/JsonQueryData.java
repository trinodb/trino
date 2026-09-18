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
package io.trino.client;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import java.io.IOException;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class JsonQueryData
        implements QueryData
{
    private final TokenBuffer buffer;
    private final long rowsCount;

    private JsonQueryData(TokenBuffer buffer, long rowsCount)
    {
        this.buffer = requireNonNull(buffer, "buffer is null");
        this.rowsCount = rowsCount;
    }

    /**
     * Buffers the JSON array at the parser's current position as a flat sequence of tokens,
     * without materializing a JsonNode tree.
     */
    public static JsonQueryData copyFrom(JsonParser parser)
            throws IOException
    {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        verify(parser.currentToken() == JsonToken.START_ARRAY, "Expected start of an array, but got %s", parser.currentToken());
        TokenBuffer buffer = new TokenBuffer(parser);
        long rowsCount = 0;
        buffer.writeStartArray();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            buffer.copyCurrentStructure(parser);
            rowsCount++;
        }
        buffer.writeEndArray();
        return new JsonQueryData(buffer, rowsCount);
    }

    public JsonParser getJsonParser()
    {
        return buffer.asParser();
    }

    void writeTo(JsonGenerator generator)
            throws IOException
    {
        buffer.serialize(generator);
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public long getRowsCount()
    {
        return rowsCount;
    }
}
