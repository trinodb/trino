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
package io.trino.server.protocol.spooling.encoding;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.client.spooling.DataAttributes;
import io.trino.server.protocol.JsonEncodingUtils.TypeEncoder;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.QueryDataEncodingConfig;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static io.trino.server.protocol.JsonEncodingUtils.createTypeEncoders;
import static io.trino.server.protocol.JsonEncodingUtils.writePagesToJsonGenerator;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class JsonQueryDataEncoder
        implements QueryDataEncoder
{
    private boolean closed;

    private static final JsonFactory JSON_FACTORY = jsonFactory();
    private static final String ENCODING = "json";
    private TypeEncoder[] typeEncoders;
    private int[] sourcePageChannels;

    public JsonQueryDataEncoder(Session session, List<OutputColumn> columns)
    {
        this.typeEncoders = createTypeEncoders(session, requireNonNull(columns, "columns is null")
                .stream()
                .map(OutputColumn::type)
                .collect(toImmutableList()));
        this.sourcePageChannels = requireNonNull(columns, "columns is null").stream()
            .mapToInt(OutputColumn::sourcePageChannel)
            .toArray();
    }

    @Override
    public DataAttributes encodeTo(OutputStream output, List<Page> pages)
            throws IOException
    {
        verify(!closed, "JsonQueryDataEncoder is already closed");
        try (CountingOutputStream wrapper = new CountingOutputStream(output); JsonGenerator generator = JSON_FACTORY.createGenerator(wrapper)) {
            writePagesToJsonGenerator(e -> { throw e; }, generator, typeEncoders, sourcePageChannels, pages);
            return DataAttributes.builder()
                    .set(SEGMENT_SIZE, toIntExact(wrapper.getCount()))
                    .build();
        }
        catch (Exception e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new IOException("Could not serialize to JSON", e);
        }
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        typeEncoders = null;
        sourcePageChannels = null;
        closed = true;
    }

    @Override
    public String encoding()
    {
        return ENCODING;
    }

    public static class Factory
            implements QueryDataEncoder.Factory
    {
        protected final JsonFactory factory;

        @Inject
        public Factory()
        {
            this.factory = jsonFactory();
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new JsonQueryDataEncoder(session, columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING;
        }
    }

    public static class ZstdFactory
            extends Factory
    {
        private final int compressionThreshold;

        @Inject
        public ZstdFactory(QueryDataEncodingConfig config)
        {
            this.compressionThreshold = toIntExact(config.getCompressionThreshold().toBytes());
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new ZstdQueryDataEncoder(super.create(session, columns), compressionThreshold);
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+zstd";
        }
    }

    public static class Lz4Factory
            extends Factory
    {
        private final int compressionThreshold;

        @Inject
        public Lz4Factory(QueryDataEncodingConfig config)
        {
            this.compressionThreshold = toIntExact(config.getCompressionThreshold().toBytes());
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new Lz4QueryDataEncoder(super.create(session, columns), compressionThreshold);
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+lz4";
        }
    }
}
