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
package io.trino.plugin.hive.rcfile;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CheckReturnValue;
import io.airlift.units.DataSize;
import io.trino.hive.formats.FileCorruptionException;
import io.trino.hive.formats.rcfile.RcFileReader;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.ObjLongConsumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static java.lang.String.format;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

public class RcFilePageSource
        implements ConnectorPageSource
{
    private static final long GUESSED_MEMORY_USAGE = DataSize.of(16, DataSize.Unit.MEGABYTE).toBytes();

    private final RcFileReader rcFileReader;

    private final List<String> columnNames;
    private final List<Type> types;

    private final Block[] constantBlocks;
    private final int[] hiveColumnIndexes;

    private int pageId;

    private boolean closed;

    public RcFilePageSource(RcFileReader rcFileReader, List<HiveColumnHandle> columns)
    {
        requireNonNull(rcFileReader, "rcFileReader is null");
        requireNonNull(columns, "columns is null");

        this.rcFileReader = rcFileReader;

        int size = columns.size();

        this.constantBlocks = new Block[size];
        this.hiveColumnIndexes = new int[size];

        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);

            namesBuilder.add(column.getName());
            typesBuilder.add(column.getType());

            hiveColumnIndexes[columnIndex] = column.getBaseHiveColumnIndex();

            if (hiveColumnIndexes[columnIndex] >= rcFileReader.getColumnCount()) {
                // this file may contain fewer fields than what's declared in the schema
                // this happens when additional columns are added to the hive table after files have been created
                constantBlocks[columnIndex] = column.getType().createNullBlock();
            }
        }
        types = typesBuilder.build();
        columnNames = namesBuilder.build();
    }

    @Override
    public long getCompletedBytes()
    {
        return rcFileReader.getBytesRead();
    }

    @Override
    public long getReadTimeNanos()
    {
        return rcFileReader.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        try {
            // advance in the current batch
            pageId++;

            // if the batch has been consumed, read the next batch
            int currentPageSize = rcFileReader.advance();
            if (currentPageSize < 0) {
                close();
                return null;
            }

            return new RcFileSourcePage(currentPageSize);
        }
        catch (TrinoException e) {
            closeAllSuppress(e, this);
            throw e;
        }
        catch (FileCorruptionException e) {
            closeAllSuppress(e, this);
            throw new TrinoException(HIVE_BAD_DATA, format("Corrupted RC file: %s", rcFileReader.getFileLocation()), e);
        }
        catch (IOException | RuntimeException e) {
            closeAllSuppress(e, this);
            throw new TrinoException(HIVE_CURSOR_ERROR, format("Failed to read RC file: %s", rcFileReader.getFileLocation()), e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        rcFileReader.close();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnNames", columnNames)
                .add("types", types)
                .toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return GUESSED_MEMORY_USAGE;
    }

    private final class RcFileSourcePage
            implements SourcePage
    {
        private final int expectedBatchId = pageId;
        private final Block[] blocks = new Block[hiveColumnIndexes.length];
        private SelectedPositions selectedPositions;

        private long sizeInBytes;
        private long retainedSizeInBytes;

        public RcFileSourcePage(int positionCount)
        {
            selectedPositions = new SelectedPositions(positionCount, null);
        }

        @Override
        public int getPositionCount()
        {
            return selectedPositions.positionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            return sizeInBytes;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return retainedSizeInBytes;
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            for (Block block : blocks) {
                if (block != null) {
                    block.retainedBytesForEachPart(consumer);
                }
            }
        }

        @Override
        public int getChannelCount()
        {
            return blocks.length;
        }

        @Override
        public Block getBlock(int channel)
        {
            checkState(pageId == expectedBatchId);
            Block block = blocks[channel];
            if (block == null) {
                if (constantBlocks[channel] != null) {
                    block = RunLengthEncodedBlock.create(constantBlocks[channel], selectedPositions.positionCount());
                }
                else {
                    try {
                        // todo use selected positions to improve read performance
                        block = rcFileReader.readBlock(hiveColumnIndexes[channel]);
                    }
                    catch (FileCorruptionException e) {
                        throw new TrinoException(HIVE_BAD_DATA, format("Corrupted RC file: %s", rcFileReader.getFileLocation()), e);
                    }
                    catch (IOException | RuntimeException e) {
                        throw new TrinoException(HIVE_CURSOR_ERROR, format("Failed to read RC file: %s", rcFileReader.getFileLocation()), e);
                    }
                    block = selectedPositions.apply(block);
                }
                blocks[channel] = block;
                sizeInBytes += block.getSizeInBytes();
                retainedSizeInBytes += block.getRetainedSizeInBytes();
            }
            return block;
        }

        @Override
        public Page getPage()
        {
            // ensure all blocks are loaded
            for (int i = 0; i < blocks.length; i++) {
                getBlock(i);
            }
            return new Page(selectedPositions.positionCount(), blocks);
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            selectedPositions = selectedPositions.selectPositions(positions, offset, size);
            retainedSizeInBytes = 0;
            for (int i = 0; i < blocks.length; i++) {
                Block block = blocks[i];
                if (block != null) {
                    block = selectedPositions.apply(block);
                    retainedSizeInBytes += block.getRetainedSizeInBytes();
                    blocks[i] = block;
                }
            }
        }
    }

    private record SelectedPositions(int positionCount, @Nullable int[] positions)
    {
        @CheckReturnValue
        public Block apply(Block block)
        {
            if (positions == null) {
                return block;
            }
            return block.getPositions(positions, 0, positionCount);
        }

        @CheckReturnValue
        public SelectedPositions selectPositions(int[] positions, int offset, int size)
        {
            if (this.positions == null) {
                for (int i = 0; i < size; i++) {
                    checkIndex(offset + i, positionCount);
                }
                return new SelectedPositions(size, Arrays.copyOfRange(positions, offset, offset + size));
            }

            int[] newPositions = new int[size];
            for (int i = 0; i < size; i++) {
                newPositions[i] = this.positions[positions[offset + i]];
            }
            return new SelectedPositions(size, newPositions);
        }
    }
}
