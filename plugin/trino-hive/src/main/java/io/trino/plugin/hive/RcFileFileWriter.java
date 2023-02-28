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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingOutputStream;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.encodings.ColumnEncodingFactory;
import io.trino.hive.formats.rcfile.RcFileWriter;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static java.util.Objects.requireNonNull;

public class RcFileFileWriter
        implements FileWriter
{
    private static final int INSTANCE_SIZE = instanceSize(RcFileFileWriter.class);
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final CountingOutputStream outputStream;
    private final AggregatedMemoryContext outputStreamMemoryContext;
    private final RcFileWriter rcFileWriter;
    private final Closeable rollbackAction;
    private final int[] fileInputColumnIndexes;
    private final List<Block> nullBlocks;
    private final Optional<Supplier<TrinoInputFile>> validationInputFactory;

    private long validationCpuNanos;

    public RcFileFileWriter(
            OutputStream outputStream,
            AggregatedMemoryContext outputStreamMemoryContext,
            Closeable rollbackAction,
            ColumnEncodingFactory columnEncodingFactory,
            List<Type> fileColumnTypes,
            Optional<CompressionKind> compressionKind,
            int[] fileInputColumnIndexes,
            Map<String, String> metadata,
            Optional<Supplier<TrinoInputFile>> validationInputFactory)
            throws IOException
    {
        this.outputStream = new CountingOutputStream(outputStream);
        this.outputStreamMemoryContext = outputStreamMemoryContext;
        rcFileWriter = new RcFileWriter(
                this.outputStream,
                fileColumnTypes,
                columnEncodingFactory,
                compressionKind,
                metadata,
                validationInputFactory.isPresent());
        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");

        this.fileInputColumnIndexes = requireNonNull(fileInputColumnIndexes, "fileInputColumnIndexes is null");

        ImmutableList.Builder<Block> nullBlocks = ImmutableList.builder();
        for (Type fileColumnType : fileColumnTypes) {
            BlockBuilder blockBuilder = fileColumnType.createBlockBuilder(null, 1, 0);
            blockBuilder.appendNull();
            nullBlocks.add(blockBuilder.build());
        }
        this.nullBlocks = nullBlocks.build();
        this.validationInputFactory = validationInputFactory;
    }

    @Override
    public long getWrittenBytes()
    {
        return outputStream.getCount();
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE + rcFileWriter.getRetainedSizeInBytes() + outputStreamMemoryContext.getBytes();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        Block[] blocks = new Block[fileInputColumnIndexes.length];
        for (int i = 0; i < fileInputColumnIndexes.length; i++) {
            int inputColumnIndex = fileInputColumnIndexes[i];
            if (inputColumnIndex < 0) {
                blocks[i] = RunLengthEncodedBlock.create(nullBlocks.get(i), dataPage.getPositionCount());
            }
            else {
                blocks[i] = dataPage.getBlock(inputColumnIndex);
            }
        }
        Page page = new Page(dataPage.getPositionCount(), blocks);
        try {
            rcFileWriter.write(page);
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public Closeable commit()
    {
        try {
            rcFileWriter.close();
        }
        catch (IOException | UncheckedIOException e) {
            try {
                rollbackAction.close();
            }
            catch (Exception ignored) {
                // ignore
            }
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }

        if (validationInputFactory.isPresent()) {
            try {
                TrinoInputFile inputFile = validationInputFactory.get().get();
                long startThreadCpuTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
                rcFileWriter.validate(inputFile);
                validationCpuNanos += THREAD_MX_BEAN.getCurrentThreadCpuTime() - startThreadCpuTime;
            }
            catch (IOException | UncheckedIOException e) {
                throw new TrinoException(HIVE_WRITE_VALIDATION_FAILED, e);
            }
        }

        return rollbackAction;
    }

    @Override
    public void rollback()
    {
        try (rollbackAction) {
            rcFileWriter.close();
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return validationCpuNanos;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("writer", rcFileWriter)
                .toString();
    }
}
