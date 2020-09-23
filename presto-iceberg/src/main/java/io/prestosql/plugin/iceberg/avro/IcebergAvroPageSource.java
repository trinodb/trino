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
package io.prestosql.plugin.iceberg.avro;

import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.orc.OrcFileWriter;
import io.prestosql.plugin.iceberg.HdfsFileIo;
import io.prestosql.plugin.iceberg.IcebergColumnHandle;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.iceberg.util.IcebergAvroDataConversion.serializeToPrestoObject;
import static java.util.Objects.requireNonNull;

public class IcebergAvroPageSource
        implements ConnectorPageSource
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcFileWriter.class).instanceSize();

    private final CloseableIterator<Record> recordIterator;

    private final List<Type> types;
    private List<org.apache.iceberg.types.Type> icebergTypes;
    private final PageBuilder pageBuilder;

    private final TimeZoneKey timeZoneKey;

    private long readBytes;
    private long readTimeNanos;
    private boolean closed;

    public IcebergAvroPageSource(
            String path,
            long start,
            long length,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            Schema fileSchema,
            List<IcebergColumnHandle> columns,
            TimeZoneKey timeZoneKey)
    {
        requireNonNull(path, "path is null");
        requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        requireNonNull(hdfsContext, "hdfsContext is null");
        requireNonNull(fileSchema, "fileSchema is null");
        requireNonNull(columns, "columns is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        List<String> columnNames = columns.stream().map(IcebergColumnHandle::getName).collect(toImmutableList());
        Schema readSchema = fileSchema.select(columnNames);
        AvroIterable<Record> avroReader = Avro.read(new HdfsFileIo(hdfsEnvironment, hdfsContext).newInputFile(path))
                .project(readSchema)
                .createReaderFunc(DataReader::create)
                .split(start, length)
                .build();

        types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .collect(toImmutableList());
        icebergTypes = readSchema.columns().stream()
                .map(Types.NestedField::type)
                .collect(toImmutableList());
        pageBuilder = new PageBuilder(types);
        recordIterator = avroReader.iterator();
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
        return closed && !recordIterator.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (closed) {
            return null;
        }
        long start = System.nanoTime();

        pageBuilder.reset();

        while (!pageBuilder.isFull() && recordIterator.hasNext()) {
            pageBuilder.declarePosition();
            Record record = recordIterator.next();
            for (int channel = 0; channel < types.size(); channel++) {
                serializeToPrestoObject(types.get(channel), icebergTypes.get(channel), pageBuilder.getBlockBuilder(channel), record.get(channel), timeZoneKey);
            }
        }
        if (!recordIterator.hasNext()) {
            close();
        }

        Page page = pageBuilder.build();
        readBytes += page.getSizeInBytes();
        readTimeNanos += System.nanoTime() - start;

        return page;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        //TODO: try to add memory used by recordIterator
        return INSTANCE_SIZE + pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            recordIterator.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
