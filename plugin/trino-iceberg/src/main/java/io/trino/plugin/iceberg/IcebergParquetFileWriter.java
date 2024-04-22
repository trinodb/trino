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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.parquet.reader.MetadataReader.createParquetMetadata;
import static io.trino.plugin.iceberg.util.ParquetUtil.footerMetrics;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class IcebergParquetFileWriter
        implements IcebergFileWriter
{
    private final MetricsConfig metricsConfig;
    private final ParquetFileWriter parquetFileWriter;
    private final Location location;

    public IcebergParquetFileWriter(
            MetricsConfig metricsConfig,
            TrinoOutputFile outputFile,
            Closeable rollbackAction,
            List<Type> fileColumnTypes,
            List<String> fileColumnNames,
            MessageType messageType,
            Map<List<String>, Type> primitiveTypes,
            ParquetWriterOptions parquetWriterOptions,
            int[] fileInputColumnIndexes,
            CompressionCodec compressionCodec,
            String trinoVersion)
            throws IOException
    {
        this.parquetFileWriter = new ParquetFileWriter(
                outputFile,
                rollbackAction,
                fileColumnTypes,
                fileColumnNames,
                messageType,
                primitiveTypes,
                parquetWriterOptions,
                fileInputColumnIndexes,
                compressionCodec,
                trinoVersion,
                Optional.empty(),
                Optional.empty());
        this.location = outputFile.location();
        this.metricsConfig = requireNonNull(metricsConfig, "metricsConfig is null");
    }

    @Override
    public Metrics getMetrics()
    {
        ParquetMetadata parquetMetadata;
        try {
            parquetMetadata = createParquetMetadata(parquetFileWriter.getFileMetadata(), new ParquetDataSourceId(location.toString()));
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Error creating metadata for Parquet file %s", location), e);
        }
        return footerMetrics(parquetMetadata, Stream.empty(), metricsConfig);
    }

    @Override
    public long getWrittenBytes()
    {
        return parquetFileWriter.getWrittenBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return parquetFileWriter.getMemoryUsage();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        parquetFileWriter.appendRows(dataPage);
    }

    @Override
    public Closeable commit()
    {
        return parquetFileWriter.commit();
    }

    @Override
    public void rollback()
    {
        parquetFileWriter.rollback();
    }

    @Override
    public long getValidationCpuNanos()
    {
        return parquetFileWriter.getValidationCpuNanos();
    }
}
