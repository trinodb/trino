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
package io.trino.plugin.iceberg;

import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Utils;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static java.util.Objects.requireNonNull;

public class IcebergPageSource
        implements ConnectorPageSource
{
    private final Block[] prefilledBlocks;
    private final int[] delegateIndexes;
    private final ConnectorPageSource delegate;

    public IcebergPageSource(
            List<IcebergColumnHandle> columns,
            Map<Integer, String> partitionKeys,
            ConnectorPageSource delegate,
            TimeZoneKey timeZoneKey)
    {
        int size = requireNonNull(columns, "columns is null").size();
        requireNonNull(partitionKeys, "partitionKeys is null");
        this.delegate = requireNonNull(delegate, "delegate is null");

        this.prefilledBlocks = new Block[size];
        this.delegateIndexes = new int[size];

        int outputIndex = 0;
        int delegateIndex = 0;
        for (IcebergColumnHandle column : columns) {
            if (partitionKeys.containsKey(column.getId())) {
                String partitionValue = partitionKeys.get(column.getId());
                Type type = column.getType();
                Object prefilledValue = deserializePartitionValue(type, partitionValue, column.getName(), timeZoneKey);
                prefilledBlocks[outputIndex] = Utils.nativeValueToBlock(type, prefilledValue);
                delegateIndexes[outputIndex] = -1;
            }
            else {
                delegateIndexes[outputIndex] = delegateIndex;
                delegateIndex++;
            }
            outputIndex++;
        }
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
                    blocks[i] = new RunLengthEncodedBlock(prefilledBlocks[i], batchSize);
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
            throw new TrinoException(ICEBERG_BAD_DATA, e);
        }
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
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
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
