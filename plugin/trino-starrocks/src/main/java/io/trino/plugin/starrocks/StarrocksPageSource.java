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
package io.trino.plugin.starrocks;

import com.starrocks.thrift.TScanBatchResult;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class StarrocksPageSource
        implements ConnectorPageSource
{
    private final StarrocksBeReader beReader;
    private final List<StarrocksColumnHandle> columnHandles;
    private final List<Type> types;

    private boolean finished;
    private long readTimeNanos;
    private long completedBytes;
    private ArrowStreamReader currentArrowReader;
    private RootAllocator rootAllocator;

    public StarrocksPageSource(StarrocksBeReader beReader, List<ColumnHandle> columns)
    {
        this.beReader = requireNonNull(beReader, "beReader is null");
        this.columnHandles = columns.stream()
                .map(StarrocksColumnHandle.class::cast)
                .collect(toImmutableList());
        this.types = columnHandles.stream()
                .map(col -> StarrocksTypeMapper.toTrinoType(col.getType(), col.getColumnType(), col.getColumnSize(), col.getDecimalDigits()))
                .collect(toImmutableList());
        this.rootAllocator = new RootAllocator(2147483647L);
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }

        long start = System.nanoTime();

        try {
            TScanBatchResult nextBatch = beReader.getNextBatch();
            if (nextBatch == null || nextBatch.eos) {
                finished = true;
                return null;
            }

            byte[] arrowData = nextBatch.getRows();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(arrowData);
            currentArrowReader = new ArrowStreamReader(byteArrayInputStream, rootAllocator);

            if (!currentArrowReader.loadNextBatch()) {
                finished = true;
                return null;
            }

            VectorSchemaRoot root = currentArrowReader.getVectorSchemaRoot();
            List<FieldVector> fieldVectors = root.getFieldVectors();
            if (fieldVectors.isEmpty() || root.getRowCount() == 0) {
                return null;
            }

            BlockBuilder[] blockBuilders = new BlockBuilder[columnHandles.size()];

            for (int i = 0; i < columnHandles.size(); i++) {
                Type type = types.get(i);
                int dataPosition = columnHandles.get(i).getOrdinalPosition() - 1;
                blockBuilders[i] = convertToTrinoBlock(fieldVectors.get(i), type, root.getRowCount(), dataPosition, blockBuilders[i]);
            }

            completedBytes += estimateBatchSize(fieldVectors);
            readTimeNanos += System.nanoTime() - start;
            beReader.setReaderOffset(beReader.getReaderOffset() + root.getRowCount());
            Block[] blocks = Arrays.stream(blockBuilders).map(BlockBuilder::build).toArray(Block[]::new);
            fieldVectors.forEach(FieldVector::clear);
            return new Page(root.getRowCount(), blocks);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read next Arrow batch", e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return rootAllocator.getAllocatedMemory();
    }

    private BlockBuilder convertToTrinoBlock(FieldVector fieldVector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
    {
        return convert(fieldVector, type, rowCount, dataPosition, blockBuilder);
    }

    private long estimateBatchSize(List<FieldVector> fieldVectors)
    {
        return fieldVectors.stream()
                .mapToLong(FieldVector::getBufferSize)
                .sum();
    }

    @Override
    public void close()
    {
        try {
            beReader.close();
            if (currentArrowReader != null) {
                currentArrowReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to close StarrocksPageSource", e);
        }
    }

    private BlockBuilder convert(FieldVector fieldVector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
    {
        return StarrocksTypeMapper.convert(fieldVector, type, rowCount, dataPosition, blockBuilder);
    }
}
