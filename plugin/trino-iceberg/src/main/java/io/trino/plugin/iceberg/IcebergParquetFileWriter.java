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

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.spi.type.Type;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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
    private final String outputPath;
    private final TrinoFileSystem fileSystem;
    private final Schema icebergSchema;

    public IcebergParquetFileWriter(
            MetricsConfig metricsConfig,
            TrinoOutputFile outputFile,
            Schema icebergSchema,
            Closeable rollbackAction,
            List<Type> fileColumnTypes,
            List<String> fileColumnNames,
            MessageType messageType,
            Map<List<String>, Type> primitiveTypes,
            ParquetWriterOptions parquetWriterOptions,
            int[] fileInputColumnIndexes,
            CompressionCodecName compressionCodecName,
            String trinoVersion,
            String outputPath,
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
                compressionCodecName,
                trinoVersion,
                Optional.empty(),
                Optional.empty());
        this.metricsConfig = requireNonNull(metricsConfig, "metricsConfig is null");
        this.outputPath = requireNonNull(outputPath, "outputPath is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.icebergSchema = icebergSchema;
    }

    @Override
    public Metrics getMetrics()
    {
        InputFile inputFile = fileSystem.toFileIo().newInputFile(outputPath);
        Metrics metrics = fileMetrics(inputFile, metricsConfig);
        Map<Integer, Long> columnSizes = metrics.columnSizes();
        Map<Integer, Long> nullValueCounts = metrics.nullValueCounts();
        ImmutableMap.Builder<Integer, Long> newColumnSizes = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, Long> newNullValueCounts = ImmutableMap.builder();
        // For complex types we need to collect sizes from their nested types and sum them up
        for (Types.NestedField field : icebergSchema.columns()) {
            if (field.type().isNestedType()) {
                newColumnSizes.put(field.fieldId(), countMetricForNestedType(field.type().asNestedType(), columnSizes, 0L));
                newNullValueCounts.put(field.fieldId(), countMetricForNestedType(field.type().asNestedType(), nullValueCounts, 0L));
            }
            else {
                newColumnSizes.put(field.fieldId(), columnSizes.get(field.fieldId()));
                newNullValueCounts.put(field.fieldId(), nullValueCounts.get(field.fieldId()));
            }
        }

        return new Metrics(
                metrics.recordCount(),
                newColumnSizes.buildOrThrow(),
                metrics.valueCounts(),
                newNullValueCounts.buildOrThrow(),
                metrics.nanValueCounts(),
                metrics.lowerBounds(),
                metrics.upperBounds());
    }

    private long countMetricForNestedType(org.apache.iceberg.types.Type.NestedType nestedType, Map<Integer, Long> counts, long result)
    {
        for (Types.NestedField nestedField : nestedType.fields()) {
            if (nestedField.type().isNestedType()) {
                countMetricForNestedType(nestedField.type().asNestedType(), counts, result);
            }
            else {
                result += counts.getOrDefault(nestedField.fieldId(), 0L);
            }
        }
        return result;
    }
}
