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

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveCompressionCodec;
import io.prestosql.plugin.iceberg.HdfsFileIo;
import io.prestosql.plugin.iceberg.IcebergFileWriter;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.io.FileAppender;

import java.io.IOException;
import java.util.List;

import static io.prestosql.plugin.hive.HiveStorageFormat.AVRO;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_CLOSE_ERROR;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_OPEN_ERROR;
import static io.prestosql.plugin.iceberg.util.IcebergAvroDataConversion.toIcebergRecords;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;

public class IcebergAvroFileWriter
        implements IcebergFileWriter
{
    private final Schema icebergSchema;
    private final List<Type> types;
    private final FileAppender<Record> avroWriter;

    private final FileSystem fileSystem;
    private final Path path;

    public IcebergAvroFileWriter(
            String tableName,
            Path path,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            Schema icebergSchema,
            List<Type> types,
            HiveCompressionCodec hiveCompressionCodec)
    {
        requireNonNull(tableName, "tableName is null");
        this.path = requireNonNull(path, "path is null");
        requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        requireNonNull(hdfsContext, "hdfsContext is null");
        this.icebergSchema = requireNonNull(icebergSchema, "icebergSchema is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        try {
            fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, path);
            avroWriter = Avro.write(new HdfsFileIo(hdfsEnvironment, hdfsContext).newOutputFile(path.toString()))
                    .schema(icebergSchema)
                    .createWriterFunc(DataWriter::create)
                    .named(tableName)
                    .set(AVRO_COMPRESSION, toIcebergAvroCompressionName(hiveCompressionCodec))
                    .build();
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_WRITER_OPEN_ERROR, "Error creating Avro file", e);
        }
    }

    @Override
    public long getWrittenBytes()
    {
        return avroWriter.length();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        //TODO: Find out a way to compute system memory usage or get a better estimation
        return AVRO.getEstimatedWriterSystemMemoryUsage().toBytes();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        for (Record record : toIcebergRecords(dataPage, types, icebergSchema)) {
            avroWriter.add(record);
        }
    }

    @Override
    public void commit()
    {
        try {
            avroWriter.close();
        }
        catch (IOException e) {
            try {
                fileSystem.delete(path, false);
            }
            catch (IOException ignored) {
            }
            throw new PrestoException(ICEBERG_WRITER_CLOSE_ERROR, "Error closing Avro file", e);
        }
    }

    @Override
    public void rollback()
    {
        try {
            try {
                avroWriter.close();
            }
            finally {
                fileSystem.delete(path, false);
            }
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_WRITER_CLOSE_ERROR, "Error rolling back write to Avro file", e);
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
        throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Unexpected hiveCompressionCodec: %s", hiveCompressionCodec.name()));
    }

    @Override
    public Metrics getMetrics()
    {
        return avroWriter.metrics();
    }
}
