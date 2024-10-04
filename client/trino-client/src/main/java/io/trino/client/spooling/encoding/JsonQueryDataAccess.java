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
package io.trino.client.spooling.encoding;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.VerifyException;
import com.google.common.io.Closer;
import io.trino.client.spooling.encoding.JsonDecodingUtils.TypeDecoder;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE;
import static com.fasterxml.jackson.core.JsonParser.Feature.USE_FAST_BIG_NUMBER_PARSER;
import static com.fasterxml.jackson.core.JsonParser.Feature.USE_FAST_DOUBLE_PARSER;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterators.unmodifiableIterator;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

class JsonQueryDataAccess
        implements QueryDataAccess
{
    private static final JsonFactory JSON_FACTORY = createJsonFactory();

    private final InputStream stream;
    private final TypeDecoder[] decoders;

    JsonQueryDataAccess(TypeDecoder[] decoders, InputStream stream)
    {
        this.decoders = requireNonNull(decoders, "decoders is null");
        this.stream = requireNonNull(stream, "stream is null");
    }

    @Override
    public Iterable<List<Object>> toIterable()
            throws IOException
    {
        return new RowWiseIterator(stream, decoders);
    }

    private static class RowWiseIterator
            implements Iterable<List<Object>>, Iterator<List<Object>>
    {
        private final Closer closer = Closer.create();
        private boolean closed;
        private final JsonParser parser;
        private final TypeDecoder[] decoders;

        public RowWiseIterator(InputStream stream, TypeDecoder[] decoders)
                throws IOException
        {
            requireNonNull(decoders, "decoders is null");
            requireNonNull(stream, "stream is null");

            this.parser = JSON_FACTORY.createParser(stream);
            this.decoders = decoders;
            closer.register(parser);
            closer.register(stream);

            // Non-empty result set starts with [[
            verify(parser.nextToken() == START_ARRAY, "Expected start of an array, but got %s", parser.currentToken());
            verify(parser.nextToken() == START_ARRAY, "Expected start of the data array, but got %s", parser.currentToken());
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
        public boolean hasNext()
        {
            return !closed;
        }

        @Override
        public List<Object> next()
        {
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
        public Iterator<List<Object>> iterator()
        {
            return unmodifiableIterator(this);
        }

        private void close()
                throws IOException
        {
            this.closed = true;
            closer.close();
        }
    }

    @SuppressModernizer // There is no JsonFactory in the client module
    private static JsonFactory createJsonFactory()
    {
        return new JsonFactory()
                .enable(USE_FAST_DOUBLE_PARSER)
                .enable(USE_FAST_BIG_NUMBER_PARSER)
                .disable(AUTO_CLOSE_SOURCE); // We want to close source explicitly
    }
}
