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

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.plugin.iceberg.fileio.ForwardingInputFile;
import io.trino.spi.type.Type;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.io.InputFile;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.parquet.ParquetUtil.fileMetrics;

public class IcebergParquetFileWriter
        extends ParquetFileWriter
        implements IcebergFileWriter
{
    private final MetricsConfig metricsConfig;
    private final InputFile inputFile;

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
            String trinoVersion,
            TrinoFileSystem fileSystem)
            throws IOException
    {
        super(outputFile,
                rollbackAction,
                fileColumnTypes,
                fileColumnNames,
                messageType,
                primitiveTypes,
                parquetWriterOptions,
                fileInputColumnIndexes,
                compressionCodec,
                trinoVersion,
                false,
                Optional.empty(),
                Optional.empty());
        this.metricsConfig = requireNonNull(metricsConfig, "metricsConfig is null");
        this.inputFile = new ForwardingInputFile(fileSystem.newInputFile(outputFile.location()));
    }

    @Override
    public Metrics getMetrics()
    {
        return fileMetrics(inputFile, metricsConfig);
    }
}
