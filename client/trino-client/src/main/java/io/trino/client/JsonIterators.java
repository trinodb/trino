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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.VerifyException;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closer;
import io.trino.client.JsonDecodingUtils.TypeDecoder;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE;
import static com.fasterxml.jackson.core.JsonParser.Feature.USE_FAST_BIG_NUMBER_PARSER;
import static com.fasterxml.jackson.core.JsonParser.Feature.USE_FAST_DOUBLE_PARSER;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.google.common.base.Verify.verify;
import static io.trino.client.JsonDecodingUtils.createTypeDecoders;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class JsonIterators
{
    private static final JsonFactory JSON_FACTORY = createJsonFactory();

    private JsonIterators() {}

    private static class JsonIterator
            extends AbstractIterator<List<Object>>
            implements CloseableIterator<List<Object>>
    {
        private final Closer closer = Closer.create();
        private boolean closed;
        private final JsonParser parser;
        private final TypeDecoder[] decoders;

        public JsonIterator(JsonParser parser, TypeDecoder[] decoders)
                throws IOException
        {
            requireNonNull(decoders, "decoders is null");

            this.parser = requireNonNull(parser, "parser is null");
            this.decoders = decoders;
            closer.register(parser);

            // Non-empty result set starts with [[
            verify(parser.nextToken() == START_ARRAY, "Expected start of an array, but got %s", parser.currentToken());

            switch (parser.nextToken()) {
                case END_ARRAY: // No data
                    closed = true;
                    break;
                case START_ARRAY:
                    // ok, we have a row
                    break;
                default:
                    throw new VerifyException("Expected start of the data array, but got " + parser.currentToken());
            }
        }

        public JsonIterator(InputStream stream, TypeDecoder[] decoders)
                throws IOException
        {
            this(JSON_FACTORY.createParser(requireNonNull(stream, "stream is null")), decoders);
            closer.register(stream);
        }

        private void checkIfClosed()
        {
            try {
                verify(parser.nextToken() == END_ARRAY, "Expected end of data array, but got %s", parser.currentToken());
                switch (parser.nextToken()) {
                    case END_ARRAY:
                        close();
                        break;
                    case START_ARRAY:
                        break;
                    default:
                        throw new VerifyException("Expected end of or start of next data array, but got " + parser.currentToken());
                }
            }
            catch (IOException e) {
                closed = true;
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public List<Object> computeNext()
        {
            if (closed) {
                return endOfData();
            }
            try {
                List<Object> row = new ArrayList<>(decoders.length);
                for (TypeDecoder decoder : decoders) {
                    if (requireNonNull(parser.nextToken()) == JsonToken.VALUE_NULL) {
                        row.add(null);
                    }
                    else {
                        row.add(decoder.decode(parser)); // allow nulls
                    }
                }
                checkIfClosed();
                return unmodifiableList(row);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close()
                throws IOException
        {
            this.closed = true;
            closer.close();
        }
    }

    public static CloseableIterator<List<Object>> forJsonParser(JsonParser parser, List<Column> columns)
            throws IOException
    {
        return new JsonIterator(parser, createTypeDecoders(columns));
    }

    public static CloseableIterator<List<Object>> forInputStream(InputStream stream, TypeDecoder[] decoders)
            throws IOException
    {
        return new JsonIterator(stream, decoders);
    }

    @SuppressModernizer // There is no JsonFactory in the client module
    static JsonFactory createJsonFactory()
    {
        return new JsonFactory()
                .setCodec(new ObjectMapper())
                .enable(USE_FAST_DOUBLE_PARSER)
                .enable(USE_FAST_BIG_NUMBER_PARSER)
                .disable(AUTO_CLOSE_SOURCE); // We want to close source explicitly
    }
}
