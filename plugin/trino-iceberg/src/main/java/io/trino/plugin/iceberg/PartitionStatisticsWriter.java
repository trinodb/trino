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

import com.google.inject.Inject;
import io.trino.spi.NodeVersion;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.InternalWriter;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableUtil.formatVersion;

/**
 * Computes the partition statistics of a snapshot and writes them to a file in the table's default file format, as the
 * Iceberg library does. The file is not registered in the table.
 */
public final class PartitionStatisticsWriter
{
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private final ExecutorService executor;

    @Inject
    public PartitionStatisticsWriter(TypeManager typeManager, NodeVersion nodeVersion, @ForIcebergPlanning ExecutorService executor)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    /**
     * Returns the new file, or empty if the snapshot has no partitions. See {@link PartitionStatisticsComputation#compute}.
     *
     * @throws IllegalArgumentException if the table's default file format is not supported for partition statistics
     * @throws PartitionStatisticsComputation.UnreadableBaseException when the base file cannot be used
     */
    public Optional<PartitionStatisticsFile> write(Table table, long snapshotId, Optional<PartitionStatisticsFile> base)
            throws IOException
    {
        FileFormat format = FileFormat.fromString(table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));
        if (format != FileFormat.AVRO && format != FileFormat.PARQUET) {
            throw new IllegalArgumentException("Partition statistics files are not supported in file format " + format);
        }
        List<GenericRecord> rows = PartitionStatisticsComputation.compute(table, snapshotId, base, typeManager, executor);
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        Schema schema = PartitionStatistics.schema(Partitioning.partitionType(table), formatVersion(table));
        String location = ((HasTableOperations) table).operations().metadataFileLocation(format.addExtension("partition-stats-%s-%s".formatted(snapshotId, UUID.randomUUID())));
        OutputFile outputFile = table.io().newOutputFile(location);
        try {
            if (format == FileFormat.AVRO) {
                writeAvro(outputFile, schema, rows);
            }
            else {
                PartitionStatisticsParquetWriter.write(outputFile, schema, rows, typeManager, nodeVersion.toString());
            }
            return Optional.of(ImmutableGenericPartitionStatisticsFile.builder()
                    .snapshotId(snapshotId)
                    .path(location)
                    .fileSizeInBytes(outputFile.toInputFile().getLength())
                    .build());
        }
        catch (IOException | RuntimeException e) {
            try {
                table.io().deleteFile(location);
            }
            catch (RuntimeException deleteFailure) {
                e.addSuppressed(deleteFailure);
            }
            throw e;
        }
    }

    private static void writeAvro(OutputFile outputFile, Schema schema, List<GenericRecord> rows)
            throws IOException
    {
        try (FileAppender<GenericRecord> appender = Avro.write(outputFile)
                .schema(schema)
                .createWriterFunc(InternalWriter::create)
                .overwrite()
                .build()) {
            appender.addAll(rows);
        }
    }
}
