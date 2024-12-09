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
package io.trino.plugin.tpch;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.function.ObjLongConsumer;

import static java.util.Objects.requireNonNull;

/**
 * Wraps pages into lazy blocks. This enables counting of materialized bytes
 * for testing purposes.
 */
class TpchPageSource
        implements ConnectorPageSource
{
    private static final int ROWS_PER_REQUEST = 4096;
    private final int maxRowsPerPage;
    private final RecordCursor cursor;
    private final List<Type> types;
    private final PageBuilder pageBuilder;
    private boolean closed;

    TpchPageSource(int maxRowsPerPage, RecordSet recordSet)
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
    public long getMemoryUsage()
    {
        return cursor.getMemoryUsage() + pageBuilder.getRetainedSizeInBytes();
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
    public SourcePage getNextSourcePage()
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
            return new TpchSourcePage(page);
        }

        return null;
    }

    private static class TpchSourcePage
            implements SourcePage
    {
        private Page page;
        private final boolean[] loaded;
        private long sizeInBytes;

        public TpchSourcePage(Page page)
        {
            this.page = requireNonNull(page, "page is null");
            this.loaded = new boolean[page.getChannelCount()];
        }

        @Override
        public int getPositionCount()
        {
            return page.getPositionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            return sizeInBytes;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return page.getRetainedSizeInBytes();
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            for (int i = 0; i < page.getChannelCount(); i++) {
                page.getBlock(i).retainedBytesForEachPart(consumer);
            }
        }

        @Override
        public int getChannelCount()
        {
            return page.getChannelCount();
        }

        @Override
        public Block getBlock(int channel)
        {
            Block block = page.getBlock(channel);
            if (!loaded[channel]) {
                loaded[channel] = true;
                sizeInBytes += block.getSizeInBytes();
            }
            return block;
        }

        @Override
        public Page getPage()
        {
            return page;
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            page = page.getPositions(positions, offset, size);
        }
    }
}
