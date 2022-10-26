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
package io.trino.plugin.raptor.legacy.storage;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcRecordReader;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.raptor.legacy.RaptorColumnHandle.SHARD_UUID_COLUMN_TYPE;
import static io.trino.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static java.util.Objects.requireNonNull;

public class RaptorPageSource
        implements ConnectorPageSource
{
    private final OrcRecordReader recordReader;
    private final List<ColumnAdaptation> columnAdaptations;
    private final OrcDataSource orcDataSource;

    private final AggregatedMemoryContext memoryContext;

    private boolean closed;

    public RaptorPageSource(
            OrcRecordReader recordReader,
            List<ColumnAdaptation> columnAdaptations,
            OrcDataSource orcDataSource,
            AggregatedMemoryContext memoryContext)
    {
        this.recordReader = requireNonNull(recordReader, "recordReader is null");
        this.columnAdaptations = ImmutableList.copyOf(requireNonNull(columnAdaptations, "columnAdaptations is null"));
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");

        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return orcDataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        Page page;
        try {
            page = recordReader.nextPage();
        }
        catch (IOException | RuntimeException e) {
            closeAllSuppress(e, this);
            throw handleException(e);
        }

        if (page == null) {
            close();
            return null;
        }

        long filePosition = recordReader.getFilePosition();
        Block[] blocks = new Block[columnAdaptations.size()];
        for (int i = 0; i < columnAdaptations.size(); i++) {
            blocks[i] = columnAdaptations.get(i).block(page, filePosition);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    public static TrinoException handleException(Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        throw new TrinoException(RAPTOR_ERROR, exception);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw new TrinoException(RAPTOR_ERROR, e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columns", columnAdaptations)
                .toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryContext.getBytes();
    }

    public interface ColumnAdaptation
    {
        Block block(Page sourcePage, long filePosition);

        static ColumnAdaptation nullColumn(Type type)
        {
            return new NullColumn(type);
        }

        static ColumnAdaptation shardUuidColumn(UUID shardUuid)
        {
            return new ShardUuidAdaptation(shardUuid);
        }

        static ColumnAdaptation bucketNumberColumn(OptionalInt bucketNumber)
        {
            if (bucketNumber.isEmpty()) {
                return nullColumn(INTEGER);
            }
            return new BucketNumberColumn(bucketNumber.getAsInt());
        }

        static ColumnAdaptation rowIdColumn()
        {
            return RowIdColumn.INSTANCE;
        }

        static ColumnAdaptation mergeRowIdColumn(OptionalInt bucketNumber, UUID shardUuid)
        {
            return new MergeRowIdColumn(bucketNumber, shardUuid);
        }

        static ColumnAdaptation sourceColumn(int index)
        {
            return new SourceColumn(index);
        }
    }

    private static class ShardUuidAdaptation
            implements ColumnAdaptation
    {
        private final Block shardUuidBlock;

        public ShardUuidAdaptation(UUID shardUuid)
        {
            Slice slice = utf8Slice(shardUuid.toString());
            BlockBuilder blockBuilder = SHARD_UUID_COLUMN_TYPE.createBlockBuilder(null, 1, slice.length());
            SHARD_UUID_COLUMN_TYPE.writeSlice(blockBuilder, slice);
            this.shardUuidBlock = blockBuilder.build();
        }

        @Override
        public Block block(Page sourcePage, long filePosition)
        {
            return RunLengthEncodedBlock.create(shardUuidBlock, sourcePage.getPositionCount());
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .toString();
        }
    }

    private static class RowIdColumn
            implements ColumnAdaptation
    {
        public static final RowIdColumn INSTANCE = new RowIdColumn();

        @Override
        public Block block(Page sourcePage, long filePosition)
        {
            int count = sourcePage.getPositionCount();
            BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(count);
            for (int i = 0; i < count; i++) {
                BIGINT.writeLong(builder, filePosition + i);
            }
            return builder.build();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .toString();
        }
    }

    private static class MergeRowIdColumn
            implements ColumnAdaptation
    {
        private final Block bucketNumberValue;
        private final Block shardUuidValue;

        public MergeRowIdColumn(OptionalInt bucketNumber, UUID shardUuid)
        {
            BlockBuilder blockBuilder = INTEGER.createFixedSizeBlockBuilder(1);
            bucketNumber.ifPresentOrElse(value -> INTEGER.writeLong(blockBuilder, value), blockBuilder::appendNull);
            bucketNumberValue = blockBuilder.build();

            BlockBuilder builder = UuidType.UUID.createFixedSizeBlockBuilder(1);
            UuidType.UUID.writeSlice(builder, javaUuidToTrinoUuid(shardUuid));
            shardUuidValue = builder.build();
        }

        @Override
        public Block block(Page sourcePage, long filePosition)
        {
            Block bucketNumberBlock = RunLengthEncodedBlock.create(bucketNumberValue, sourcePage.getPositionCount());
            Block shardUuidBlock = RunLengthEncodedBlock.create(shardUuidValue, sourcePage.getPositionCount());
            Block rowIdBlock = RowIdColumn.INSTANCE.block(sourcePage, filePosition);
            return RowBlock.fromFieldBlocks(
                    sourcePage.getPositionCount(),
                    Optional.empty(),
                    new Block[] {bucketNumberBlock, shardUuidBlock, rowIdBlock});
        }
    }

    private static class NullColumn
            implements ColumnAdaptation
    {
        private final Type type;
        private final Block nullBlock;

        public NullColumn(Type type)
        {
            this.type = requireNonNull(type, "type is null");
            this.nullBlock = type.createBlockBuilder(null, 1, 0)
                    .appendNull()
                    .build();
        }

        @Override
        public Block block(Page sourcePage, long filePosition)
        {
            return RunLengthEncodedBlock.create(nullBlock, sourcePage.getPositionCount());
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", type)
                    .toString();
        }
    }

    private static class BucketNumberColumn
            implements ColumnAdaptation
    {
        private final Block bucketNumberBlock;

        public BucketNumberColumn(int bucketNumber)
        {
            BlockBuilder blockBuilder = INTEGER.createFixedSizeBlockBuilder(1);
            INTEGER.writeLong(blockBuilder, bucketNumber);
            this.bucketNumberBlock = blockBuilder.build();
        }

        @Override
        public Block block(Page sourcePage, long filePosition)
        {
            return RunLengthEncodedBlock.create(bucketNumberBlock, sourcePage.getPositionCount());
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .toString();
        }
    }

    private static class SourceColumn
            implements ColumnAdaptation
    {
        private final int index;

        public SourceColumn(int index)
        {
            checkArgument(index >= 0, "index is negative");
            this.index = index;
        }

        @Override
        public Block block(Page sourcePage, long filePosition)
        {
            return sourcePage.getBlock(index);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("index", index)
                    .toString();
        }
    }
}
