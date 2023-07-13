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
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.iceberg.IcebergAvroDataConversion.toIcebergRecords;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_CLOSE_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_OPEN_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;

public class IcebergAvroFileWriter
        implements IcebergFileWriter
{
    private static final int INSTANCE_SIZE = instanceSize(IcebergAvroFileWriter.class);

    // Use static table name instead of the actual name because it becomes outdated once the table is renamed
    public static final String AVRO_TABLE_NAME = "table";

    private final Schema icebergSchema;
    private final List<Type> types;
    private final FileAppender<Record> avroWriter;
    private final Closeable rollbackAction;

    public IcebergAvroFileWriter(
            OutputFile file,
            Closeable rollbackAction,
            Schema icebergSchema,
            List<Type> types,
            HiveCompressionCodec hiveCompressionCodec)
    {
        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction null");
        this.icebergSchema = requireNonNull(icebergSchema, "icebergSchema is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        try {
            avroWriter = Avro.write(file)
                    .schema(icebergSchema)
                    .createWriterFunc(DataWriter::create)
                    .named(AVRO_TABLE_NAME)
                    .set(AVRO_COMPRESSION, toIcebergAvroCompressionName(hiveCompressionCodec))
                    .build();
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_WRITER_OPEN_ERROR, "Error creating Avro file: " + file.location(), e);
        }
    }

    @Override
    public long getWrittenBytes()
    {
        return avroWriter.length();
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public void appendRows(Page dataPage)
    {
        for (Record record : toIcebergRecords(dataPage, types, icebergSchema)) {
            avroWriter.add(record);
        }
    }

    @Override
    public Closeable commit()
    {
        try {
            avroWriter.close();
        }
        catch (IOException e) {
            try {
                rollbackAction.close();
            }
            catch (Exception ex) {
                if (!e.equals(ex)) {
                    e.addSuppressed(ex);
                }
            }
            throw new TrinoException(ICEBERG_WRITER_CLOSE_ERROR, "Error closing Avro file", e);
        }

        return rollbackAction;
    }

    @Override
    public void rollback()
    {
        try (rollbackAction) {
            avroWriter.close();
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_WRITER_CLOSE_ERROR, "Error rolling back write to Avro file", e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }

    private static String toIcebergAvroCompressionName(HiveCompressionCodec hiveCompressionCodec)
    {
        switch (hiveCompressionCodec) {
            case NONE:
                return "UNCOMPRESSED";
            case SNAPPY:
                return "SNAPPY";
            case LZ4:
                return "LZ4";
            case ZSTD:
                return "ZSTD";
            case GZIP:
                return "GZIP";
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected hiveCompressionCodec: " + hiveCompressionCodec);
    }

    @Override
    public Metrics getMetrics()
    {
        return avroWriter.metrics();
    }
}
