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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.io.CountingInputStream;
import io.trino.spi.PageBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.LongSupplier;

import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static java.util.Objects.requireNonNull;

public class EsriReader
        implements Closeable
{
    private static final JsonFactory JSON_FACTORY = jsonFactory();

    private final CountingInputStream inputStream;
    private final PageBuilder pageBuilder;
    private final EsriDeserializer esriDeserializer;
    private final LongSupplier rawInputPositionSupplier;
    private JsonParser parser;
    private long readTimeNanos;
    private boolean closed;

    private static final String FEATURES_ARRAY_NAME = "features";

    public EsriReader(InputStream inputStream, PageBuilder pageBuilder, EsriDeserializer esriDeserializer)
            throws IOException
    {
        requireNonNull(inputStream, "inputStream is null");
        this.inputStream = new CountingInputStream(inputStream);
        this.rawInputPositionSupplier = this.inputStream::getCount;
        this.pageBuilder = requireNonNull(pageBuilder, "pageBuilder is null");
        this.esriDeserializer = requireNonNull(esriDeserializer, "esriDeserializer is null");

        this.initializeParser();
    }

    private void initializeParser()
            throws IOException
    {
        parser = JSON_FACTORY.createParser(inputStream);

        // Find features array
        while (true) {
            JsonToken token = parser.nextToken();
            if (token == null) {
                return;
            }
            if (token == JsonToken.START_ARRAY &&
                    FEATURES_ARRAY_NAME.equals(parser.currentName())) {
                break;
            }
        }
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

    public boolean next()
            throws IOException
    {
        long start = System.nanoTime();

        try {
            JsonToken token = parser.nextToken();
            if (token == JsonToken.START_OBJECT && parser.currentName() == null) {
                esriDeserializer.deserialize(pageBuilder, parser);
                return true;
            }
            else {
                return false;
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
