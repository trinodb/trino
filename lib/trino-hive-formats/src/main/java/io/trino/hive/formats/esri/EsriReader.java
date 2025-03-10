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
package io.trino.hive.formats.esri;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.CountingInputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.LongSupplier;

import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static java.util.Objects.requireNonNull;

public class EsriReader
        implements Closeable
{
    private final CountingInputStream inputStream;
    private final LongSupplier rawInputPositionSupplier;
    private JsonParser parser;
    private long readTimeNanos;
    private boolean closed;

    private static final String FEATURES_ARRAY_NAME = "features";

    public EsriReader(InputStream inputStream)
    {
        requireNonNull(inputStream, "inputStream is null");
        this.inputStream = new CountingInputStream(inputStream);
        this.rawInputPositionSupplier = this.inputStream::getCount;
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        if (inputStream != null) {
            inputStream.close();
        }
        if (parser != null) {
            parser.close();
        }
    }

    public String next()
            throws IOException
    {
        long start = System.nanoTime();

        try {
            if (parser == null) {
                parser = jsonFactory().createParser(inputStream);
                parser.setCodec(new ObjectMapper());

                JsonToken token = parser.nextToken();
                while (token != null) {
                    boolean isStartArray = token == JsonToken.START_ARRAY;
                    String currentName = parser.currentName();
                    boolean isFeaturesArray = currentName != null &&
                            currentName.equals(FEATURES_ARRAY_NAME);

                    if (isStartArray && isFeaturesArray) {
                        break;
                    }

                    token = parser.nextToken();
                }

                if (token == null) {
                    return null;
                }
            }

            JsonToken token = parser.nextToken();
            if (token == JsonToken.START_OBJECT && parser.currentName() == null) {
                ObjectNode node = parser.readValueAsTree();
                return node.toString();
            }
            else {
                return null;
            }
        }
        finally {
            long duration = System.nanoTime() - start;
            readTimeNanos += duration;
        }
    }

    public long getBytesRead()
    {
        return rawInputPositionSupplier.getAsLong();
    }

    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    public boolean isClosed()
    {
        return closed;
    }
}
