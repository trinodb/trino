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
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.reader.ParquetReader;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParquetPageSource
        implements ConnectorPageSource
{
    private final ParquetReader parquetReader;
    private final List<Type> types;
    private final List<Optional<Field>> fields;

    private int batchId;
    private boolean closed;

    public ParquetPageSource(ParquetReader parquetReader, List<Type> types, List<Optional<Field>> fields)
    {
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
    }

    @Override
    public long getCompletedBytes()
    {
        return parquetReader.getDataSource().getReadBytes();
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
    public long getSystemMemoryUsage()
    {
        return parquetReader.getSystemMemoryContext().getBytes();
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

            Block[] blocks = new Block[fields.size()];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                Optional<Field> field = fields.get(fieldId);
                if (field.isPresent()) {
                    blocks[fieldId] = new LazyBlock(batchSize, new ParquetBlockLoader(field.get()));
                }
                else {
                    blocks[fieldId] = RunLengthEncodedBlock.create(types.get(fieldId), null, batchSize);
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (TrinoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throw new TrinoException(HIVE_CURSOR_ERROR, e);
        }
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (e != throwable) {
                throwable.addSuppressed(e);
            }
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
        private final int expectedBatchId = batchId;
        private final Field field;
        private boolean loaded;

        public ParquetBlockLoader(Field field)
        {
            this.field = requireNonNull(field, "field is null");
        }

        @Override
        public final Block load()
        {
            checkState(!loaded, "Already loaded");
            checkState(batchId == expectedBatchId);

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
}
