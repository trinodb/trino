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
package io.trino.plugin.deltalake;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.Utils;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_MODIFIED_TIME_TYPE;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_SIZE_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_SIZE_TYPE;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.PATH_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.PATH_TYPE;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.ROW_ID_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_DATA;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.deserializePartitionValue;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DeltaLakePageSource
        implements ConnectorPageSource
{
    private static final JsonCodec<List<String>> PARTITIONS_CODEC = new JsonCodecFactory().listJsonCodec(String.class);

    private final Block[] prefilledBlocks;
    private final int[] delegateIndexes;
    private final int rowIdIndex;
    private final Block pathBlock;
    private final Block partitionsBlock;
    private final ConnectorPageSource delegate;

    public DeltaLakePageSource(
            List<DeltaLakeColumnHandle> columns,
            Set<String> missingColumnNames,
            Map<String, Optional<String>> partitionKeys,
            List<String> partitionValues,
            ConnectorPageSource delegate,
            String path,
            long fileSize,
            long fileModifiedTime)
    {
        int size = columns.size();
        requireNonNull(partitionKeys, "partitionKeys is null");
        this.delegate = requireNonNull(delegate, "delegate is null");

        this.prefilledBlocks = new Block[size];
        this.delegateIndexes = new int[size];

        int outputIndex = 0;
        int delegateIndex = 0;

        int rowIdIndex = -1;
        Block pathBlock = null;
        Block partitionsBlock = null;

        for (DeltaLakeColumnHandle column : columns) {
            if (partitionKeys.containsKey(column.getPhysicalName())) {
                Type type = column.getType();
                Object prefilledValue = deserializePartitionValue(column, partitionKeys.get(column.getPhysicalName()));
                prefilledBlocks[outputIndex] = Utils.nativeValueToBlock(type, prefilledValue);
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getName().equals(PATH_COLUMN_NAME)) {
                prefilledBlocks[outputIndex] = Utils.nativeValueToBlock(PATH_TYPE, utf8Slice(path));
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getName().equals(FILE_SIZE_COLUMN_NAME)) {
                prefilledBlocks[outputIndex] = Utils.nativeValueToBlock(FILE_SIZE_TYPE, fileSize);
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getName().equals(FILE_MODIFIED_TIME_COLUMN_NAME)) {
                long packedTimestamp = packDateTimeWithZone(fileModifiedTime, UTC_KEY);
                prefilledBlocks[outputIndex] = Utils.nativeValueToBlock(FILE_MODIFIED_TIME_TYPE, packedTimestamp);
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getName().equals(ROW_ID_COLUMN_NAME)) {
                rowIdIndex = outputIndex;
                pathBlock = Utils.nativeValueToBlock(VARCHAR, utf8Slice(path));
                partitionsBlock = Utils.nativeValueToBlock(VARCHAR, wrappedBuffer(PARTITIONS_CODEC.toJsonBytes(partitionValues)));
                delegateIndexes[outputIndex] = delegateIndex;
                delegateIndex++;
            }
            else if (missingColumnNames.contains(column.getName())) {
                prefilledBlocks[outputIndex] = Utils.nativeValueToBlock(column.getType(), null);
                delegateIndexes[outputIndex] = -1;
            }
            else {
                delegateIndexes[outputIndex] = delegateIndex;
                delegateIndex++;
            }
            outputIndex++;
        }

        this.rowIdIndex = rowIdIndex;
        this.pathBlock = pathBlock;
        this.partitionsBlock = partitionsBlock;
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }
            int batchSize = dataPage.getPositionCount();
            Block[] blocks = new Block[prefilledBlocks.length];
            for (int i = 0; i < prefilledBlocks.length; i++) {
                if (prefilledBlocks[i] != null) {
                    blocks[i] = RunLengthEncodedBlock.create(prefilledBlocks[i], batchSize);
                }
                else if (i == rowIdIndex) {
                    blocks[i] = createRowIdBlock(dataPage.getBlock(delegateIndexes[i]));
                }
                else {
                    blocks[i] = dataPage.getBlock(delegateIndexes[i]);
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(DELTA_LAKE_BAD_DATA, e);
        }
    }

    private Block createRowIdBlock(Block rowIndexBlock)
    {
        int positions = rowIndexBlock.getPositionCount();
        Block[] fields = {
                RunLengthEncodedBlock.create(pathBlock, positions),
                rowIndexBlock,
                RunLengthEncodedBlock.create(partitionsBlock, positions),
        };
        return RowBlock.fromFieldBlocks(positions, Optional.empty(), fields);
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }
}
