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
import io.trino.spi.block.VariantBlock;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class VariantColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(VariantColumnWriter.class);

    private final ColumnWriter metadataColumnWriter;
    private final ColumnWriter valueColumnWriter;
    private final int maxDefinitionLevel;

    public VariantColumnWriter(ColumnWriter metadataColumnWriter, ColumnWriter valueColumnWriter, int maxDefinitionLevel)
    {
        this.metadataColumnWriter = requireNonNull(metadataColumnWriter, "metadataColumnWriter is null");
        this.valueColumnWriter = requireNonNull(valueColumnWriter, "valueColumnWriter is null");
        this.maxDefinitionLevel = maxDefinitionLevel;
    }

    @Override
    public void writeBlock(ColumnChunk columnChunk)
            throws IOException
    {
        Block block = columnChunk.getBlock();
        // This must be a VariantBlock
        VariantBlock.VariantNestedBlocks nested = VariantBlock.getNullSuppressedNestedFields(block);

        Block metadataBlock = nested.metadataBlock();
        Block valueBlock = nested.valueBlock();

        checkArgument(
                metadataBlock.getPositionCount() == valueBlock.getPositionCount(),
                "metadata and value blocks must have the same position count");

        // IMPORTANT: we add a VARIANT-level def/rep provider here,
        // just like StructColumnWriter does for RowBlock.
        List<DefLevelWriterProvider> defLevelWriterProviders = ImmutableList.<DefLevelWriterProvider>builder()
                .addAll(columnChunk.getDefLevelWriterProviders())
                .add(DefLevelWriterProviders.of(block, maxDefinitionLevel))
                .build();

        List<RepLevelWriterProvider> repLevelWriterProviders = ImmutableList.<RepLevelWriterProvider>builder()
                .addAll(columnChunk.getRepLevelWriterProviders())
                .add(RepLevelWriterProviders.of(block))
                .build();

        // Push the two leaf blocks down with the augmented providers
        metadataColumnWriter.writeBlock(new ColumnChunk(metadataBlock, defLevelWriterProviders, repLevelWriterProviders));
        valueColumnWriter.writeBlock(new ColumnChunk(valueBlock, defLevelWriterProviders, repLevelWriterProviders));
    }

    @Override
    public void close()
    {
        metadataColumnWriter.close();
        valueColumnWriter.close();
    }

    @Override
    public List<BufferData> getBuffer()
            throws IOException
    {
        ImmutableList.Builder<BufferData> builder = ImmutableList.builder();
        builder.addAll(metadataColumnWriter.getBuffer());
        builder.addAll(valueColumnWriter.getBuffer());
        return builder.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return metadataColumnWriter.getBufferedBytes() + valueColumnWriter.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + metadataColumnWriter.getRetainedBytes() + valueColumnWriter.getRetainedBytes();
    }
}
