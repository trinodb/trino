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
import com.google.common.io.Closer;
import com.google.common.io.CountingInputStream;
import io.trino.spi.PageBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import static com.fasterxml.jackson.core.JsonFactory.Feature.INTERN_FIELD_NAMES;
import static io.trino.hive.formats.esri.EsriDeserializer.invalidJson;
import static io.trino.hive.formats.esri.EsriDeserializer.nextObjectField;
import static io.trino.hive.formats.esri.EsriDeserializer.nextTokenRequired;
import static io.trino.hive.formats.esri.EsriDeserializer.skipCurrentValue;
import static io.trino.plugin.base.util.JsonUtils.jsonFactoryBuilder;
import static java.util.Objects.requireNonNull;

public final class EsriReader
        implements Closeable
{
    private static final JsonFactory JSON_FACTORY = jsonFactoryBuilder()
            .disable(INTERN_FIELD_NAMES)
            .build();
    private static final String FEATURES_NAME = "features";

    private final CountingInputStream inputStream;
    private final EsriDeserializer esriDeserializer;
    private final JsonParser parser;

    private long readTimeNanos;
    private boolean closed;

    public EsriReader(InputStream inputStream, EsriDeserializer esriDeserializer)
            throws IOException
    {
        this.inputStream = new CountingInputStream(requireNonNull(inputStream, "inputStream is null"));
        this.esriDeserializer = requireNonNull(esriDeserializer, "esriDeserializer is null");

        parser = JSON_FACTORY.createParser(this.inputStream);
        if (nextTokenRequired(parser) != JsonToken.START_OBJECT) {
            throw invalidJson("File must start with a JSON object");
        }

        // Advance to the features field
        while (nextObjectField(parser)) {
            String fieldName = parser.currentName();
            JsonToken fieldValue = nextTokenRequired(parser);
            if (FEATURES_NAME.equals(fieldName)) {
                // read the array start token
                if (fieldValue == JsonToken.VALUE_NULL) {
                    close();
                    return;
                }
                if (fieldValue != JsonToken.START_ARRAY) {
                    throw invalidJson("Features field must be an array");
                }
                break;
            }
            else {
                // skip the field value
                skipCurrentValue(parser);
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        try (Closer closer = Closer.create()) {
            closer.register(inputStream);
            closer.register(parser);
        }
    }

    public boolean next(PageBuilder pageBuilder)
            throws IOException
    {
        if (closed) {
            return false;
        }

        long start = System.nanoTime();
        try {
            JsonToken token = parser.nextToken();
            if (token == null || token == JsonToken.END_ARRAY) {
                // everything after the features array is ignored
                close();
                return false;
            }
            esriDeserializer.deserialize(pageBuilder, parser);
            return true;
        }
        finally {
            long duration = System.nanoTime() - start;
            readTimeNanos += duration;
        }
    }

    public long getBytesRead()
    {
        return inputStream.getCount();
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
