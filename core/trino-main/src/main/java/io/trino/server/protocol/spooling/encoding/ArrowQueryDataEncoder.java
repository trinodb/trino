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

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.client.spooling.DataAttributes;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.QueryDataEncodingConfig;
import io.trino.server.protocol.spooling.encoding.ArrowWriters.ColumnWriter;
import io.trino.spi.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.server.protocol.spooling.encoding.ArrowWriters.createWriter;
import static io.trino.server.protocol.spooling.encoding.ArrowWriters.toField;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Encodes each segment as a single Arrow IPC stream: one schema message followed by one record batch per page.
 */
public class ArrowQueryDataEncoder
        implements QueryDataEncoder
{
    private static final String ENCODING = "arrow";

    private final BufferAllocator allocator;
    private final List<OutputColumn> columns;
    private final Schema schema;
    private boolean closed;

    public ArrowQueryDataEncoder(BufferAllocator parentAllocator, String name, List<OutputColumn> columns)
    {
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        // Fails the query on unsupported types before any data is produced
        this.schema = new Schema(columns.stream()
                .map(column -> toField(column.columnName(), column.type()))
                .toList());
        this.allocator = parentAllocator.newChildAllocator(name, 0, Long.MAX_VALUE);
    }

    @Override
    public DataAttributes encodeTo(OutputStream output, List<Page> pages)
            throws IOException
    {
        verify(!closed, "ArrowQueryDataEncoder is already closed");
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(output))) {
            ColumnWriter[] writers = new ColumnWriter[columns.size()];
            for (int i = 0; i < writers.length; i++) {
                writers[i] = createWriter(root.getVector(i), columns.get(i).type());
            }
            writer.start();
            for (Page page : pages) {
                root.allocateNew();
                for (int i = 0; i < writers.length; i++) {
                    writers[i].write(page.getBlock(columns.get(i).sourcePageChannel()));
                }
                root.setRowCount(page.getPositionCount());
                writer.writeBatch();
            }
            writer.end();
            return DataAttributes.builder()
                    .set(SEGMENT_SIZE, toIntExact(writer.bytesWritten()))
                    .build();
        }
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        allocator.close();
    }

    @Override
    public String encoding()
    {
        return ENCODING;
    }

    public static class Factory
            implements QueryDataEncoder.Factory
    {
        // Native memory is released when each segment is written, so the allocator is not bounded
        private final BufferAllocator rootAllocator = new RootAllocator();

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new ArrowQueryDataEncoder(rootAllocator, session.getQueryId().toString(), columns);
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
