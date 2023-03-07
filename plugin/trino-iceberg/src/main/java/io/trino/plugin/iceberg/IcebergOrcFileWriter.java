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
import io.trino.orc.OrcDataSink;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcWriteValidation;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcType;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.orc.OrcFileWriter;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.type.Type;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.OrcMetrics;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static java.util.Objects.requireNonNull;

public class IcebergOrcFileWriter
        extends OrcFileWriter
        implements IcebergFileWriter
{
    private final MetricsConfig metricsConfig;
    private final String outputPath;
    private final TrinoFileSystem fileSystem;

    public IcebergOrcFileWriter(
            MetricsConfig metricsConfig,
            OrcDataSink orcDataSink,
            Closeable rollbackAction,
            List<String> columnNames,
            List<Type> fileColumnTypes,
            ColumnMetadata<OrcType> fileColumnOrcTypes,
            CompressionKind compression,
            OrcWriterOptions options,
            int[] fileInputColumnIndexes,
            Map<String, String> metadata,
            Optional<Supplier<OrcDataSource>> validationInputFactory,
            OrcWriteValidation.OrcWriteValidationMode validationMode,
            OrcWriterStats stats,
            String outputPath,
            TrinoFileSystem fileSystem)
    {
        super(orcDataSink, WriterKind.INSERT, NO_ACID_TRANSACTION, false, OptionalInt.empty(), rollbackAction, columnNames, fileColumnTypes, fileColumnOrcTypes, compression, options, fileInputColumnIndexes, metadata, validationInputFactory, validationMode, stats);
        this.metricsConfig = requireNonNull(metricsConfig, "metricsConfig is null");
        this.outputPath = requireNonNull(outputPath, "outputPath is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
    }

    @Override
    public Metrics getMetrics()
    {
        InputFile inputFile = new ForwardingFileIo(fileSystem).newInputFile(outputPath);
        return OrcMetrics.fromInputFile(inputFile, metricsConfig);
    }
}
