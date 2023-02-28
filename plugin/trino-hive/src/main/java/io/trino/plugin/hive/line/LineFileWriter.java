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
package io.trino.plugin.hive.line;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.trino.hive.formats.line.LineSerializer;
import io.trino.hive.formats.line.LineWriter;
import io.trino.plugin.hive.FileWriter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static java.util.Objects.requireNonNull;

public class LineFileWriter
        implements FileWriter
{
    private static final int INSTANCE_SIZE = instanceSize(LineFileWriter.class);

    private final LineWriter lineWriter;
    private final LineSerializer serializer;
    private final Closeable rollbackAction;
    private final int[] fileInputColumnIndexes;
    private final List<Block> nullBlocks;

    private final DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);

    public LineFileWriter(LineWriter lineWriter, LineSerializer serializer, Closeable rollbackAction, int[] fileInputColumnIndexes)
    {
        this.lineWriter = requireNonNull(lineWriter, "lineWriter is null");
        this.serializer = requireNonNull(serializer, "serializer is null");
        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");

        this.fileInputColumnIndexes = requireNonNull(fileInputColumnIndexes, "fileInputColumnIndexes is null");

        ImmutableList.Builder<Block> nullBlocks = ImmutableList.builder();
        for (Type fileColumnType : serializer.getTypes()) {
            BlockBuilder blockBuilder = fileColumnType.createBlockBuilder(null, 1, 0);
            blockBuilder.appendNull();
            nullBlocks.add(blockBuilder.build());
        }
        this.nullBlocks = nullBlocks.build();
    }

    @Override
    public long getWrittenBytes()
    {
        return lineWriter.getWrittenBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE + lineWriter.getRetainedSizeInBytes() + sliceOutput.getRetainedSize();
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
            for (int position = 0; position < page.getPositionCount(); position++) {
                sliceOutput.reset();
                serializer.write(page, position, sliceOutput);
                lineWriter.write(sliceOutput.slice());
            }
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public Closeable commit()
    {
        try {
            lineWriter.close();
        }
        catch (Exception e) {
            try {
                rollbackAction.close();
            }
            catch (Exception ignored) {
                // ignore
            }
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }
        return rollbackAction;
    }

    @Override
    public void rollback()
    {
        try (rollbackAction) {
            lineWriter.close();
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }
}
