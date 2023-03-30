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
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
    private final BigQueryReadClient bigQueryReadClient;
    private final BigQuerySplit split;
    private final Iterator<ReadRowsResponse> responses;
    private final BigQueryArrowToPageConverter bigQueryArrowToPageConverter;
    private final BufferAllocator streamBufferAllocator;
    private final PageBuilder pageBuilder;

    public BigQueryStorageArrowPageSource(
            BigQueryReadClient bigQueryReadClient,
            int maxReadRowsRetries,
            BigQuerySplit split,
            List<BigQueryColumnHandle> columns)
    {
        this.bigQueryReadClient = requireNonNull(bigQueryReadClient, "bigQueryReadClient is null");
        this.split = requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");
        Schema schema = deserializeSchema(split.getSchemaString());
        log.debug("Starting to read from %s", split.getStreamName());
        responses = new ReadRowsHelper(bigQueryReadClient, split.getStreamName(), maxReadRowsRetries).readRows();
        this.streamBufferAllocator = allocator.newChildAllocator(split.getStreamName(), 1024, Long.MAX_VALUE);
        this.bigQueryArrowToPageConverter = new BigQueryArrowToPageConverter(streamBufferAllocator, schema, columns);
        this.pageBuilder = new PageBuilder(columns.stream()
                .map(BigQueryColumnHandle::getTrinoType)
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
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return !responses.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");
        if (!responses.hasNext()) {
            return null;
        }
        ReadRowsResponse response = responses.next();
        try (ArrowRecordBatch batch = deserializeResponse(streamBufferAllocator, response)) {
            bigQueryArrowToPageConverter.convert(pageBuilder, batch);
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    @Override
    public long getMemoryUsage()
    {
        long memoryUsage = streamBufferAllocator.getAllocatedMemory();
        if (split.getDataSize().isPresent()) {
            memoryUsage += split.getDataSize().getAsInt() + pageBuilder.getSizeInBytes();
        }
        return memoryUsage;
    }

    @Override
    public void close()
    {
        streamBufferAllocator.close();
        bigQueryArrowToPageConverter.close();
        bigQueryReadClient.close();
    }

    private ArrowRecordBatch deserializeResponse(BufferAllocator allocator, ReadRowsResponse response)
    {
        int serializedSize = response.getArrowRecordBatch().getSerializedSize();
        long totalReadSize = readBytes.addAndGet(serializedSize);
        log.debug("Read %d bytes (total %d) from %s", serializedSize, totalReadSize, split.getStreamName());

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
