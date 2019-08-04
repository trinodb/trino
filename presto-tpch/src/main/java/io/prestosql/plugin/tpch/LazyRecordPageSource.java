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
package io.prestosql.plugin.tpch;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Wraps pages into lazy blocks. This enables counting of materialized bytes
 * for testing purposes.
 */
class LazyRecordPageSource
        implements ConnectorPageSource
{
    private static final int ROWS_PER_REQUEST = 4096;
    private final int maxRowsPerPage;
    private final RecordCursor cursor;
    private final List<Type> types;
    private final PageBuilder pageBuilder;
    private boolean closed;

    LazyRecordPageSource(int maxRowsPerPage, RecordSet recordSet)
    {
        requireNonNull(recordSet, "recordSet is null");

        this.maxRowsPerPage = maxRowsPerPage;
        this.cursor = recordSet.cursor();
        this.types = ImmutableList.copyOf(recordSet.getColumnTypes());
        this.pageBuilder = new PageBuilder(this.types);
    }

    @Override
    public long getCompletedBytes()
    {
        return cursor.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return cursor.getReadTimeNanos();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return cursor.getSystemMemoryUsage() + pageBuilder.getSizeInBytes();
    }

    @Override
    public void close()
    {
        closed = true;
        cursor.close();
    }

    @Override
    public boolean isFinished()
    {
        return closed && pageBuilder.isEmpty();
    }

    @Override
    public Page getNextPage()
    {
        if (!closed) {
            for (int i = 0; i < ROWS_PER_REQUEST && !pageBuilder.isFull() && pageBuilder.getPositionCount() < maxRowsPerPage; i++) {
                if (!cursor.advanceNextPosition()) {
                    closed = true;
                    break;
                }

                pageBuilder.declarePosition();
                for (int column = 0; column < types.size(); column++) {
                    BlockBuilder output = pageBuilder.getBlockBuilder(column);
                    if (cursor.isNull(column)) {
                        output.appendNull();
                    }
                    else {
                        Type type = types.get(column);
                        Class<?> javaType = type.getJavaType();
                        if (javaType == boolean.class) {
                            type.writeBoolean(output, cursor.getBoolean(column));
                        }
                        else if (javaType == long.class) {
                            type.writeLong(output, cursor.getLong(column));
                        }
                        else if (javaType == double.class) {
                            type.writeDouble(output, cursor.getDouble(column));
                        }
                        else if (javaType == Slice.class) {
                            Slice slice = cursor.getSlice(column);
                            type.writeSlice(output, slice, 0, slice.length());
                        }
                        else {
                            type.writeObject(output, cursor.getObject(column));
                        }
                    }
                }
            }
        }

        if ((closed && !pageBuilder.isEmpty()) || pageBuilder.isFull() || pageBuilder.getPositionCount() >= maxRowsPerPage) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return lazyWrapper(page);
        }

        return null;
    }

    private Page lazyWrapper(Page page)
    {
        Block[] lazyBlocks = new Block[page.getChannelCount()];
        for (int i = 0; i < page.getChannelCount(); ++i) {
            Block block = page.getBlock(i);
            lazyBlocks[i] = new LazyBlock(page.getPositionCount(), lazyBlock -> lazyBlock.setBlock(block));
        }

        return new Page(page.getPositionCount(), lazyBlocks);
    }
}
