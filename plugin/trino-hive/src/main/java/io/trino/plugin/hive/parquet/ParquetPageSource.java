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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.reader.ParquetReader;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class ParquetPageSource
        implements ConnectorPageSource
{
    private final ParquetReader parquetReader;
    private final List<Type> types;
    private final List<Optional<Field>> fields;
    /**
     * Indicates whether the column at each index should be populated with the
     * indices of its rows
     */
    private final List<Boolean> rowIndexLocations;

    private int batchId;
    private boolean closed;
    private long completedPositions;

    public ParquetPageSource(ParquetReader parquetReader, List<Type> types, List<Optional<Field>> fields)
    {
        this(parquetReader, types, nCopies(types.size(), false), fields);
    }

    /**
     * @param types Column types
     * @param rowIndexLocations Whether each column should be populated with the indices of its rows
     * @param fields List of field descriptions. Empty optionals will result in columns populated with {@code NULL}
     */
    public ParquetPageSource(
            ParquetReader parquetReader,
            List<Type> types,
            List<Boolean> rowIndexLocations,
            List<Optional<Field>> fields)
    {
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.rowIndexLocations = requireNonNull(rowIndexLocations, "rowIndexLocations is null");
        this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));

        // TODO: Instead of checking that the three list arguments go together correctly,
        //   we should do something like the ORC reader's ColumnAdatpation, using
        //   subclasses that contain only the necessary information for each column.
        checkArgument(
                types.size() == rowIndexLocations.size() && types.size() == fields.size(),
                "types, rowIndexLocations, and fields must correspond one-to-one-to-one");
        Streams.forEachPair(
                rowIndexLocations.stream(),
                fields.stream(),
                (isIndexColumn, field) -> checkArgument(
                        !(isIndexColumn && field.isPresent()),
                        "Field info for row index column must be empty Optional"));
    }

    private boolean isIndexColumn(int column)
    {
        return rowIndexLocations.get(column);
    }

    @Override
    public long getCompletedBytes()
    {
        return parquetReader.getDataSource().getReadBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return parquetReader.getDataSource().getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public long getMemoryUsage()
    {
        return parquetReader.getMemoryContext().getBytes();
    }

    @Override
    public Page getNextPage()
    {
        try {
            batchId++;
            int batchSize = parquetReader.nextBatch();

            if (closed || batchSize <= 0) {
                close();
                return null;
            }

            completedPositions += batchSize;

            Block[] blocks = new Block[fields.size()];
            for (int column = 0; column < blocks.length; column++) {
                if (isIndexColumn(column)) {
                    blocks[column] = getRowIndexColumn(parquetReader.lastBatchStartRow(), batchSize);
                }
                else {
                    Type type = types.get(column);
                    blocks[column] = fields.get(column)
                            .<Block>map(field -> new LazyBlock(batchSize, new ParquetBlockLoader(field)))
                            .orElseGet(() -> RunLengthEncodedBlock.create(type, null, batchSize));
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (TrinoException e) {
            closeAllSuppress(e, this);
            throw e;
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, this);
            throw new TrinoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            parquetReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final class ParquetBlockLoader
            implements LazyBlockLoader
    {
        /**
         * Stores batch ID at instantiation time. Loading fails if the ID
         * changes before {@link #load()} is called.
         */
        private final int expectedBatchId = batchId;
        private final Field field;
        private boolean loaded;

        public ParquetBlockLoader(Field field)
        {
            this.field = requireNonNull(field, "field is null");
        }

        @Override
        public Block load()
        {
            checkState(!loaded, "Already loaded");
            checkState(batchId == expectedBatchId, "Inconsistent state; wrong batch");

            Block block;
            String parquetDataSourceId = parquetReader.getDataSource().getId().toString();
            try {
                block = parquetReader.readBlock(field);
            }
            catch (ParquetCorruptionException e) {
                throw new TrinoException(HIVE_BAD_DATA, format("Corrupted parquet data; source=%s; %s", parquetDataSourceId, e.getMessage()), e);
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_CURSOR_ERROR, format("Failed reading parquet data; source= %s; %s", parquetDataSourceId, e.getMessage()), e);
            }

            loaded = true;
            return block;
        }
    }

    private static Block getRowIndexColumn(long baseIndex, int size)
    {
        long[] rowIndices = new long[size];
        for (int position = 0; position < size; position++) {
            rowIndices[position] = baseIndex + position;
        }
        return new LongArrayBlock(size, Optional.empty(), rowIndices);
    }
}
