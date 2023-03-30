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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.IcebergAvroDataConversion.serializeToTrinoBlock;
import static java.util.Objects.requireNonNull;

public class IcebergAvroPageSource
        implements ConnectorPageSource
{
    private final CloseableIterator<Record> recordIterator;

    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final Map<String, org.apache.iceberg.types.Type> icebergTypes;
    /**
     * Indicates whether the column at each index should be populated with the
     * indices of its rows
     */
    private final List<Boolean> rowIndexLocations;
    private final PageBuilder pageBuilder;
    private final AggregatedMemoryContext memoryUsage;

    private int rowId;
    private long readBytes;
    private long readTimeNanos;

    public IcebergAvroPageSource(
            InputFile file,
            long start,
            long length,
            Schema fileSchema,
            Optional<NameMapping> nameMapping,
            List<String> columnNames,
            List<Type> columnTypes,
            List<Boolean> rowIndexLocations,
            AggregatedMemoryContext memoryUsage)
    {
        this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.rowIndexLocations = ImmutableList.copyOf(requireNonNull(rowIndexLocations, "rowIndexLocations is null"));
        this.memoryUsage = requireNonNull(memoryUsage, "memoryUsage is null");
        checkArgument(
                columnNames.size() == rowIndexLocations.size() && columnNames.size() == columnTypes.size(),
                "names, rowIndexLocations, and types must correspond one-to-one-to-one");

        // The column orders in the generated schema might be different from the original order
        Schema readSchema = fileSchema.select(columnNames);
        Avro.ReadBuilder builder = Avro.read(file)
                .project(readSchema)
                .createReaderFunc(DataReader::create)
                .split(start, length);
        nameMapping.ifPresent(builder::withNameMapping);
        AvroIterable<Record> avroReader = builder.build();
        icebergTypes = readSchema.columns().stream()
                .collect(toImmutableMap(Types.NestedField::name, Types.NestedField::type));
        pageBuilder = new PageBuilder(columnTypes);
        recordIterator = avroReader.iterator();
        // TODO: Remove when NPE check has been released: https://github.com/trinodb/trino/issues/15372
        isFinished();
    }

    private boolean isIndexColumn(int column)
    {
        return rowIndexLocations.get(column);
    }

    @Override
    public long getCompletedBytes()
    {
        return readBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return !recordIterator.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (!recordIterator.hasNext()) {
            return null;
        }
        long start = System.nanoTime();

        pageBuilder.reset();

        while (!pageBuilder.isFull() && recordIterator.hasNext()) {
            pageBuilder.declarePosition();
            Record record = recordIterator.next();
            for (int channel = 0; channel < columnTypes.size(); channel++) {
                if (isIndexColumn(channel)) {
                    pageBuilder.getBlockBuilder(channel).writeLong(rowId);
                }
                else {
                    String name = columnNames.get(channel);
                    serializeToTrinoBlock(columnTypes.get(channel), icebergTypes.get(name), pageBuilder.getBlockBuilder(channel), record.getField(name));
                }
            }
            rowId++;
        }

        Page page = pageBuilder.build();
        readBytes += page.getSizeInBytes();
        readTimeNanos += System.nanoTime() - start;

        return page;
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsage.getBytes();
    }

    @Override
    public void close()
    {
        try {
            recordIterator.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
