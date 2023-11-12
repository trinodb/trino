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
import io.trino.spi.type.Type;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.iceberg.util.OrcMetrics.computeMetrics;
import static java.util.Objects.requireNonNull;

public class IcebergOrcFileWriter
        extends OrcFileWriter
        implements IcebergFileWriter
{
    private final Schema icebergSchema;
    private final ColumnMetadata<OrcType> orcColumns;
    private final MetricsConfig metricsConfig;

    public IcebergOrcFileWriter(
            MetricsConfig metricsConfig,
            Schema icebergSchema,
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
            OrcWriterStats stats)
    {
        super(orcDataSink, WriterKind.INSERT, NO_ACID_TRANSACTION, false, OptionalInt.empty(), rollbackAction, columnNames, fileColumnTypes, fileColumnOrcTypes, compression, options, fileInputColumnIndexes, metadata, validationInputFactory, validationMode, stats);
        this.icebergSchema = requireNonNull(icebergSchema, "icebergSchema is null");
        this.metricsConfig = requireNonNull(metricsConfig, "metricsConfig is null");
        orcColumns = fileColumnOrcTypes;
    }

    @Override
    public Metrics getMetrics()
    {
        return computeMetrics(metricsConfig, icebergSchema, orcColumns, orcWriter.getFileRowCount(), orcWriter.getFileStats());
    }
}
