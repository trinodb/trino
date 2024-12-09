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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.protobuf.ByteString;
import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

public class BigQueryStorageArrowPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(BigQueryStorageArrowPageSource.class);

    private static final BufferAllocator allocator = new RootAllocator(RootAllocator
            .configBuilder()
            .from(RootAllocator.defaultConfig())
            .maxAllocation(Integer.MAX_VALUE)
            .build());

    private final AtomicLong readBytes = new AtomicLong();
    private final AtomicLong readTimeNanos = new AtomicLong();
    private final BigQueryReadClient bigQueryReadClient;
    private final ExecutorService executor;
    private final String streamName;
    private final BigQueryArrowToPageConverter bigQueryArrowToPageConverter;
    private final BufferAllocator streamBufferAllocator;
    private final PageBuilder pageBuilder;
    private final Iterator<ReadRowsResponse> responses;

    private CompletableFuture<ReadRowsResponse> nextResponse;
    private boolean finished;

    public BigQueryStorageArrowPageSource(
            BigQueryTypeManager typeManager,
            BigQueryReadClient bigQueryReadClient,
            ExecutorService executor,
            int maxReadRowsRetries,
            BigQuerySplit split,
            List<BigQueryColumnHandle> columns)
    {
        this.bigQueryReadClient = requireNonNull(bigQueryReadClient, "bigQueryReadClient is null");
        this.executor = requireNonNull(executor, "executor is null");
        requireNonNull(split, "split is null");
        this.streamName = split.streamName();
        requireNonNull(columns, "columns is null");
        Schema schema = deserializeSchema(split.schemaString());
        log.debug("Starting to read from %s", split.streamName());
        responses = new ReadRowsHelper(bigQueryReadClient, split.streamName(), maxReadRowsRetries).readRows();
        nextResponse = CompletableFuture.supplyAsync(this::getResponse, executor);
        this.streamBufferAllocator = allocator.newChildAllocator(split.streamName(), 1024, Long.MAX_VALUE);
        this.bigQueryArrowToPageConverter = new BigQueryArrowToPageConverter(typeManager, streamBufferAllocator, schema, columns);
        this.pageBuilder = new PageBuilder(columns.stream()
                .map(BigQueryColumnHandle::trinoType)
                .collect(toImmutableList()));
    }

    @Override
    public long getCompletedBytes()
    {
        return readBytes.get();
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos.get();
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");
        ReadRowsResponse response;
        try {
            response = getFutureValue(nextResponse);
        }
        catch (NoSuchElementException ignored) {
            finished = true;
            return null;
        }
        nextResponse = CompletableFuture.supplyAsync(this::getResponse, executor);
        long start = System.nanoTime();
        try (ArrowRecordBatch batch = deserializeResponse(streamBufferAllocator, response)) {
            bigQueryArrowToPageConverter.convert(pageBuilder, batch);
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        readTimeNanos.addAndGet(System.nanoTime() - start);
        return SourcePage.create(page);
    }

    @Override
    public long getMemoryUsage()
    {
        return streamBufferAllocator.getAllocatedMemory() + pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close()
    {
        streamBufferAllocator.close();
        bigQueryArrowToPageConverter.close();
        nextResponse.cancel(true);
        bigQueryReadClient.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return nextResponse;
    }

    private ReadRowsResponse getResponse()
    {
        long start = System.nanoTime();
        ReadRowsResponse response = responses.next();
        readTimeNanos.addAndGet(System.nanoTime() - start);
        return response;
    }

    private ArrowRecordBatch deserializeResponse(BufferAllocator allocator, ReadRowsResponse response)
    {
        int serializedSize = response.getArrowRecordBatch().getSerializedSize();
        long totalReadSize = readBytes.addAndGet(serializedSize);
        log.debug("Read %d bytes (total %d) from %s", serializedSize, totalReadSize, streamName);

        try {
            return MessageSerializer.deserializeRecordBatch(readChannelForByteString(response.getArrowRecordBatch().getSerializedRecordBatch()), allocator);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error deserializing next Arrow Batch", e);
        }
    }

    private static ReadChannel readChannelForByteString(ByteString input)
    {
        return new ReadChannel(new ByteArrayReadableSeekableByteChannel(input.toByteArray()));
    }

    private static Schema deserializeSchema(String schema)
    {
        try {
            return Schema.fromJSON(schema);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
