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
import io.airlift.units.DataSize;
import io.trino.hive.formats.FileCorruptionException;
import io.trino.hive.formats.rcfile.RcFileReader;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveType;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RcFilePageSource
        implements ConnectorPageSource
{
    private static final long GUESSED_MEMORY_USAGE = DataSize.of(16, DataSize.Unit.MEGABYTE).toBytes();

    private static final int NULL_ENTRY_SIZE = 0;
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
        ImmutableList.Builder<HiveType> hiveTypesBuilder = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);

            namesBuilder.add(column.getName());
            typesBuilder.add(column.getType());
            hiveTypesBuilder.add(column.getHiveType());

            hiveColumnIndexes[columnIndex] = column.getBaseHiveColumnIndex();

            if (hiveColumnIndexes[columnIndex] >= rcFileReader.getColumnCount()) {
                // this file may contain fewer fields than what's declared in the schema
                // this happens when additional columns are added to the hive table after files have been created
                BlockBuilder blockBuilder = column.getType().createBlockBuilder(null, 1, NULL_ENTRY_SIZE);
                blockBuilder.appendNull();
                constantBlocks[columnIndex] = blockBuilder.build();
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
    public Page getNextPage()
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

            Block[] blocks = new Block[hiveColumnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = RunLengthEncodedBlock.create(constantBlocks[fieldId], currentPageSize);
                }
                else {
                    blocks[fieldId] = createBlock(currentPageSize, fieldId);
                }
            }

            return new Page(currentPageSize, blocks);
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

    private Block createBlock(int currentPageSize, int fieldId)
    {
        int hiveColumnIndex = hiveColumnIndexes[fieldId];

        return new LazyBlock(
                currentPageSize,
                new RcFileBlockLoader(hiveColumnIndex));
    }

    private final class RcFileBlockLoader
            implements LazyBlockLoader
    {
        private final int expectedBatchId = pageId;
        private final int columnIndex;
        private boolean loaded;

        public RcFileBlockLoader(int columnIndex)
        {
            this.columnIndex = columnIndex;
        }

        @Override
        public Block load()
        {
            checkState(!loaded, "Already loaded");
            checkState(pageId == expectedBatchId);

            Block block;
            try {
                block = rcFileReader.readBlock(columnIndex);
            }
            catch (FileCorruptionException e) {
                throw new TrinoException(HIVE_BAD_DATA, format("Corrupted RC file: %s", rcFileReader.getFileLocation()), e);
            }
            catch (IOException | RuntimeException e) {
                throw new TrinoException(HIVE_CURSOR_ERROR, format("Failed to read RC file: %s", rcFileReader.getFileLocation()), e);
            }

            loaded = true;
            return block;
        }
    }
}
