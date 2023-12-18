/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.parallel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugins.snowflake.parallel.writer.BlockWriter;
import com.starburstdata.trino.plugins.snowflake.parallel.writer.BlockWriterFactory;
import com.starburstdata.trino.plugins.snowflake.parallel.writer.ConverterFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.BufferAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.RootAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.FieldVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ValueVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.VectorSchemaRoot;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ipc.ArrowStreamReader;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeParallelSessionProperties.getQuotedIdentifiersIgnoreCase;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.function.UnaryOperator.identity;

public class SnowflakeArrowPageSource
        implements ConnectorPageSource
{
    private static final RootAllocator ROOT_ALLOCATOR = new RootAllocator();
    private final BufferAllocator bufferAllocator;
    private final boolean quotedIdentifiersIgnoreCase;
    private final PageBuilder pageBuilder;
    private final List<JdbcColumnHandle> columns;
    private final StarburstDataConversionContext conversionContext;
    private final ChunkFetcher fetcher;
    private final long splitRetainedSize;
    private long completedBytes;
    private CompletableFuture<byte[]> chunkFuture;
    private boolean finished;

    public SnowflakeArrowPageSource(ConnectorSession session, SnowflakeArrowSplit split, List<JdbcColumnHandle> columns, StarburstResultStreamProvider streamProvider)
    {
        this.splitRetainedSize = requireNonNull(split, "split is null").getRetainedSizeInBytes();
        this.columns = requireNonNull(columns, "columns is null");
        this.quotedIdentifiersIgnoreCase = getQuotedIdentifiersIgnoreCase(requireNonNull(session, "session is null"));

        this.pageBuilder = new PageBuilder(columns.stream()
                .map(JdbcColumnHandle::getColumnType)
                .collect(toImmutableList()));

        this.bufferAllocator = ROOT_ALLOCATOR.newChildAllocator(
                "snowflakeArrowSplit" + split.hashCode(),
                // Allocator is used sequentially, largest chunk is what it will need to hold at most at the same time
                split.getLargestChunkUncompressedBytes(),
                Long.MAX_VALUE);

        int[] decimalColumnScales = columns.stream()
                .map(column -> column.getJdbcTypeHandle().getDecimalDigits()
                        .orElse(0))
                .mapToInt(Integer::intValue)
                .toArray();

        this.conversionContext = new StarburstDataConversionContext(
                split.snowflakeSessionParameters(),
                decimalColumnScales,
                split.resultVersion());

        this.fetcher = new ChunkFetcher(requireNonNull(streamProvider, "streamProvider is null"), split.chunks());
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return fetcher.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return requireNonNullElse(chunkFuture, NOT_BLOCKED);
    }

    @Override
    public Page getNextPage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");

        if (finished) {
            return null;
        }

        // getNextPage is not called concurrently hence there is no need for synchronization here
        if (chunkFuture == null) {
            chunkFuture = fetcher.fetchNextChunk();
            return null;
        }

        try {
            processChunk(chunkFuture.join());
        }
        catch (CompletionException e) {
            throw new TrinoException(JDBC_ERROR, "Failed fetching Arrow chunk", e);
        }

        // fetcher might be 'done', but page source is not 'finished' until fetched result is consumed
        finished = fetcher.isDone();
        if (!finished) {
            chunkFuture = fetcher.fetchNextChunk();
        }
        Page page = pageBuilder.build();
        // A single split maps to a multiple chunk files,
        // each holding up to a certain amount of records (from a few hundred up to a few million)
        pageBuilder.reset();
        completedBytes += page.getSizeInBytes();

        return page;
    }

    private void processChunk(byte[] chunk)
    {
        try (CloseableArrowBatch batch = decodeArrowInputStream(chunk)) {
            for (List<ValueVector> vectors : batch.batch()) {
                int columnCount = columns.size();
                checkState(!vectors.isEmpty(), "There must be at least one vector in the batch of vectors");
                pageBuilder.declarePositions(vectors.get(0).getValueCount());
                Map<Integer, Integer> columnToVectorOrder = buildColumnOrder(vectors);
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    BlockWriter writer = createWriter(vectors.get(columnToVectorOrder.get(columnIndex)), columnIndex);
                    writer.write(pageBuilder.getBlockBuilder(columnIndex));
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(JDBC_ERROR, "Failed reading Arrow stream", e);
        }
        catch (SFException e) {
            throw new TrinoException(JDBC_ERROR, "Couldn't write Snowflake blocks", e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return bufferAllocator.getAllocatedMemory() + splitRetainedSize + pageBuilder.getSizeInBytes();
    }

    @Override
    public void close()
    {
        fetcher.close();
        bufferAllocator.close();
    }

    private BlockWriter createWriter(ValueVector vector, int columnIndex)
    {
        ArrowVectorConverter converter = ConverterFactory.createSnowflakeConverter(vector, columnIndex, conversionContext);
        JdbcColumnHandle columnHandle = columns.get(columnIndex);
        int rowCount = vector.getValueCount();
        return BlockWriterFactory.createWriter(columnHandle, converter, rowCount);
    }

    private Map<Integer, Integer> buildColumnOrder(List<ValueVector> vectors)
    {
        Map<String, Integer> vectorIndexes = IntStream.range(0, vectors.size())
                .boxed()
                // In case of a collision, e.g. MyVector and Myvector, the ImmutableMap will throw and such tables can't be queried
                // with Trino until https://github.com/trinodb/trino/issues/17
                .collect(toImmutableMap(i -> vectors.get(i).getField().getName(), identity()));

        ImmutableMap.Builder<Integer, Integer> columnToVectorOrder = ImmutableMap.builderWithExpectedSize(columns.size());
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            String columnName = columns.get(columnIndex).getColumnName();
            Integer vectorIndex = vectorIndexes.get(quotedIdentifiersIgnoreCase ? columnName.toUpperCase(ENGLISH) : columnName);
            if (vectorIndex == null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot find corresponding vector for column %s. Trino columns: %s, vectors: %s".formatted(
                        columnName,
                        columns,
                        vectorIndexes));
            }
            columnToVectorOrder.put(columnIndex, vectorIndex);
        }

        return columnToVectorOrder.buildOrThrow();
    }

    private CloseableArrowBatch decodeArrowInputStream(byte[] data)
            throws IOException
    {
        try (ArrowStreamReader reader = new ArrowStreamReader(wrap(data), bufferAllocator); VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot()) {
            ImmutableList.Builder<List<ValueVector>> batchBuilder = ImmutableList.builder();
            while (reader.loadNextBatch()) {
                ImmutableList.Builder<ValueVector> vectorBuilder = ImmutableList.builderWithExpectedSize(vectorSchemaRoot.getFieldVectors().size());
                for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
                    // transfer will not copy data but transfer ownership of memory, otherwise values will be gone
                    // once reader is gone
                    TransferPair transferPair = fieldVector.getTransferPair(bufferAllocator);
                    transferPair.transfer();
                    vectorBuilder.add(transferPair.getTo());
                }
                batchBuilder.add(vectorBuilder.build());
                vectorSchemaRoot.clear();
            }
            return new CloseableArrowBatch(batchBuilder.build());
        }
    }

    private SeekableByteChannel wrap(byte[] data)
    {
        return new ByteArrayReadableSeekableByteChannel(data);
    }

    @SuppressWarnings("UnusedVariable") // error-prone false positive
    private record CloseableArrowBatch(List<List<ValueVector>> batch)
            implements AutoCloseable
    {
        @Override
        public void close()
        {
            for (List<ValueVector> vectors : batch) {
                vectors.forEach(ValueVector::close);
            }
        }
    }
}
