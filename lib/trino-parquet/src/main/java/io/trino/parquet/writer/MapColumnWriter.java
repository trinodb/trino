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
import io.trino.spi.block.ColumnarMap;

import java.io.IOException;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class MapColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(MapColumnWriter.class);

    private final ColumnWriter keyWriter;
    private final ColumnWriter valueWriter;
    private final int maxDefinitionLevel;
    private final int maxRepetitionLevel;

    public MapColumnWriter(ColumnWriter keyWriter, ColumnWriter valueWriter, int maxDefinitionLevel, int maxRepetitionLevel)
    {
        this.keyWriter = requireNonNull(keyWriter, "keyWriter is null");
        this.valueWriter = requireNonNull(valueWriter, "valueWriter is null");
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.maxRepetitionLevel = maxRepetitionLevel;
    }

    @Override
    public void writeBlock(ColumnChunk columnChunk)
            throws IOException
    {
        ColumnarMap columnarMap = ColumnarMap.toColumnarMap(columnChunk.getBlock());

        List<DefLevelWriterProvider> defLevelWriterProviders = ImmutableList.<DefLevelWriterProvider>builder()
                .addAll(columnChunk.getDefLevelWriterProviders())
                .add(DefLevelWriterProviders.of(columnarMap, maxDefinitionLevel)).build();

        List<RepLevelWriterProvider> repLevelIterables = ImmutableList.<RepLevelWriterProvider>builder()
                .addAll(columnChunk.getRepLevelWriterProviders())
                .add(RepLevelWriterProviders.of(columnarMap, maxRepetitionLevel)).build();

        keyWriter.writeBlock(new ColumnChunk(columnarMap.getKeysBlock(), defLevelWriterProviders, repLevelIterables));
        valueWriter.writeBlock(new ColumnChunk(columnarMap.getValuesBlock(), defLevelWriterProviders, repLevelIterables));
    }

    @Override
    public void close()
    {
        keyWriter.close();
        valueWriter.close();
    }

    @Override
    public List<BufferData> getBuffer()
            throws IOException
    {
        return ImmutableList.<BufferData>builder().addAll(keyWriter.getBuffer()).addAll(valueWriter.getBuffer()).build();
    }

    @Override
    public long getBufferedBytes()
    {
        return keyWriter.getBufferedBytes() + valueWriter.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + keyWriter.getRetainedBytes() + valueWriter.getRetainedBytes();
    }
}
