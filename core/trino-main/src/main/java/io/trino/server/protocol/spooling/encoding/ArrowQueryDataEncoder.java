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
import io.trino.server.protocol.spooling.ForArrowEncoder;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.airlift.log.Logger;
import io.trino.server.protocol.spooling.encoding.arrow.PageWriter;
import io.trino.server.protocol.spooling.encoding.arrow.TrinoToArrowTypeConverter;
import io.trino.spi.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.server.protocol.spooling.encoding.arrow.TrinoToArrowTypeConverter.toArrowField;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ArrowQueryDataEncoder
        implements QueryDataEncoder
{
    private static final Logger log = Logger.get(ArrowQueryDataEncoder.class);
    private static final String ENCODING = "arrow";

    private final List<Field> fields;
    private final List<OutputColumn> columns;
    private final CompressionCodec.Factory compressionFactory;
    private final CompressionUtil.CodecType codecType;
    private final BufferAllocator allocator;
    private final Optional<Semaphore> semaphore;

    public ArrowQueryDataEncoder(
            BufferAllocator allocator,
            CompressionCodec.Factory compressionFactory,
            CompressionUtil.CodecType codecType,
            List<OutputColumn> columns,
            Optional<Semaphore> semaphore)
    {
        this.fields = columns.stream()
                .map(column -> toArrowField(column.columnName(), column.type(), true))
                .collect(toImmutableList());
        this.columns = requireNonNull(columns, "columns is null");

        this.compressionFactory = requireNonNull(compressionFactory, "compressionFactory is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.codecType = requireNonNull(codecType, "codecType is null");
        this.semaphore = requireNonNull(semaphore, "semaphore is null");
    }

    @Override
    public DataAttributes encodeTo(OutputStream output, List<Page> pages)
            throws IOException
    {
        // Acquire semaphore to protect Arrow off-heap memory allocation
        if (semaphore.isPresent()) {
            acquireArrowSemaphore(semaphore.get());
        }
        try {
            // Arrow vectors (off-heap unsafe memory) allocated and released within this try block
            try (VectorSchemaRoot schema = VectorSchemaRoot.create(new Schema(fields), allocator)) {
                ArrowStreamWriter streamWriter = new ArrowStreamWriter(schema, null, Channels.newChannel(output), IpcOption.DEFAULT, compressionFactory, codecType);
                try (PageWriter arrowPageWriter = new PageWriter(streamWriter, schema, columns)) {
                    return DataAttributes.builder()
                            .set(SEGMENT_SIZE, toIntExact(arrowPageWriter.writePages(pages)))
                            .build();
                }
            }
        }
        finally {
            // Release semaphore immediately after Arrow unsafe buffers are freed
            if (semaphore.isPresent()) {
                releaseArrowSemaphore(semaphore.get());
            }
        }
    }

    @Override
    public String encoding()
    {
        return switch (codecType) {
            case NO_COMPRESSION -> ENCODING;
            case ZSTD -> ENCODING + "+zstd";
            default -> throw new IllegalArgumentException("Unsupported codec type: " + codecType);
        };
    }

    @Override
    public void close()
    {
        allocator.close();
    }

    private void acquireArrowSemaphore(Semaphore semaphore)
    {
        long threadId = Thread.currentThread().getId();
        // Log detailed semaphore state before acquiring
        int availablePermits = semaphore.availablePermits();
        int queueLength = semaphore.getQueueLength();
        boolean hasQueuedThreads = semaphore.hasQueuedThreads();
        log.debug("Thread %d - Arrow semaphore state before acquire - Available permits: %d, Queue length: %d, Has queued threads: %s", 
                threadId, availablePermits, queueLength, hasQueuedThreads);
        
        // Warn if there's Arrow serialization backpressure
        if (queueLength > 0) {
            log.warn("Thread %d - Arrow serialization backpressure detected with %d threads waiting. " +
                    "Consider increasing 'protocol.spooling.arrow.max-concurrent-serialization' (current: %d available permits)", 
                    threadId, queueLength, availablePermits);
        }
        
        semaphore.acquireUninterruptibly();
        
        // Log state after successful acquisition
        log.debug("Thread %d - Arrow semaphore acquired successfully - Available permits now: %d, Queue length: %d", 
                threadId, semaphore.availablePermits(), semaphore.getQueueLength());
    }

    private void releaseArrowSemaphore(Semaphore semaphore)
    {
        long threadId = Thread.currentThread().getId();
        // Log state before release
        log.debug("Thread %d - Releasing Arrow semaphore - Available permits before release: %d, Queue length: %d", 
                threadId, semaphore.availablePermits(), semaphore.getQueueLength());
        
        semaphore.release();
        
        // Log state after release
        log.debug("Thread %d - Arrow semaphore released - Available permits after release: %d, Queue length: %d, Has queued threads: %s",
                threadId, semaphore.availablePermits(), semaphore.getQueueLength(), semaphore.hasQueuedThreads());
    }

    public static class Factory
            implements QueryDataEncoder.Factory
    {
        private final BufferAllocator allocator;
        private final Optional<Semaphore> semaphore;

        @Inject
        public Factory(BufferAllocator rootAllocator, @ForArrowEncoder Optional<Semaphore> semaphore)
        {
            this.allocator = requireNonNull(rootAllocator, "allocator is null");
            this.semaphore = requireNonNull(semaphore, "semaphore is null");
        }

        @Override
        public List<OutputColumn> unsupported(List<OutputColumn> columns)
        {
            return TrinoToArrowTypeConverter.unsupported(columns);
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new ArrowQueryDataEncoder(
                    allocator.newChildAllocator(session.getQueryId().toString(), 0, Long.MAX_VALUE),
                    NoCompressionCodec.Factory.INSTANCE,
                    CompressionUtil.CodecType.NO_COMPRESSION,
                    columns,
                    semaphore);
        }

        @Override
        public String encoding()
        {
            return ENCODING;
        }
    }

    public static class ZstdFactory
            implements QueryDataEncoder.Factory
    {
        private final BufferAllocator allocator;
        private final CompressionCodec.Factory compressionFactory;
        private final Optional<Semaphore> semaphore;

        @Inject
        public ZstdFactory(BufferAllocator rootAllocator, CompressionCodec.Factory compressionFactory, @ForArrowEncoder Optional<Semaphore> semaphore)
        {
            this.allocator = requireNonNull(rootAllocator, "allocator is null");
            this.compressionFactory = requireNonNull(compressionFactory, "compressionFactory is null");
            this.semaphore = requireNonNull(semaphore, "semaphore is null");
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new ArrowQueryDataEncoder(
                    allocator.newChildAllocator(session.getQueryId().toString(), 0, Long.MAX_VALUE),
                    compressionFactory,
                    CompressionUtil.CodecType.ZSTD,
                    columns,
                    semaphore);
        }

        @Override
        public List<OutputColumn> unsupported(List<OutputColumn> columns)
        {
            return TrinoToArrowTypeConverter.unsupported(columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING + "+zstd";
        }
    }
}
