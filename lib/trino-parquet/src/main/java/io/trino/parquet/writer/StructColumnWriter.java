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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.trino.parquet.writer.repdef.DefLevelWriterProvider;
import io.trino.parquet.writer.repdef.DefLevelWriterProviders;
import io.trino.parquet.writer.repdef.RepLevelWriterProvider;
import io.trino.parquet.writer.repdef.RepLevelWriterProviders;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;

import java.io.IOException;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.Preconditions.checkArgument;

public class StructColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(StructColumnWriter.class);

    private final List<ColumnWriter> columnWriters;
    private final int maxDefinitionLevel;

    public StructColumnWriter(List<ColumnWriter> columnWriters, int maxDefinitionLevel)
    {
        this.columnWriters = requireNonNull(columnWriters, "columnWriters is null");
        this.maxDefinitionLevel = maxDefinitionLevel;
    }

    @Override
    public void writeBlock(ColumnChunk columnChunk)
            throws IOException
    {
        Block block = columnChunk.getBlock();
        List<Block> fields = RowBlock.getNullSuppressedRowFieldsFromBlock(block);
        checkArgument(fields.size() == columnWriters.size(), "Row field size %s is not equal to columnWriters size %s", fields.size(), columnWriters.size());

        List<DefLevelWriterProvider> defLevelWriterProviders = ImmutableList.<DefLevelWriterProvider>builder()
                .addAll(columnChunk.getDefLevelWriterProviders())
                .add(DefLevelWriterProviders.of(block, maxDefinitionLevel))
                .build();
        List<RepLevelWriterProvider> repLevelWriterProviders = ImmutableList.<RepLevelWriterProvider>builder()
                .addAll(columnChunk.getRepLevelWriterProviders())
                .add(RepLevelWriterProviders.of(block))
                .build();

        for (int i = 0; i < columnWriters.size(); ++i) {
            ColumnWriter columnWriter = columnWriters.get(i);
            Block field = fields.get(i);
            columnWriter.writeBlock(new ColumnChunk(field, defLevelWriterProviders, repLevelWriterProviders));
        }
    }

    @Override
    public void close()
    {
        columnWriters.forEach(ColumnWriter::close);
    }

    @Override
    public List<BufferData> getBuffer()
            throws IOException
    {
        ImmutableList.Builder<BufferData> builder = ImmutableList.builder();
        for (ColumnWriter columnWriter : columnWriters) {
            builder.addAll(columnWriter.getBuffer());
        }
        return builder.build();
    }

    @Override
    public long getBufferedBytes()
    {
        // Avoid using streams here for performance reasons
        long bufferedBytes = 0;
        for (ColumnWriter columnWriter : columnWriters) {
            bufferedBytes += columnWriter.getBufferedBytes();
        }
        return bufferedBytes;
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE +
                columnWriters.stream().mapToLong(ColumnWriter::getRetainedBytes).sum();
    }
}
