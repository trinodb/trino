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

import io.airlift.log.Logger;
import io.trino.orc.OrcDataSink;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.trino.orc.OrcWriter;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcType;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_CLOSE_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_DATA_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITE_VALIDATION_FAILED;
import static io.trino.plugin.iceberg.util.OrcMetrics.computeMetrics;
import static java.util.Objects.requireNonNull;

public class IcebergOrcFileWriter
        implements IcebergFileWriter
{
    private static final Logger log = Logger.get(IcebergOrcFileWriter.class);
    private static final int INSTANCE_SIZE = instanceSize(IcebergOrcFileWriter.class);
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final OrcWriter orcWriter;
    private final Schema icebergSchema;
    private final ColumnMetadata<OrcType> orcColumns;
    private final MetricsConfig metricsConfig;
    private final Closeable rollbackAction;
    private final int[] fileInputColumnIndexes;
    private final List<Block> nullBlocks;
    private final Optional<Supplier<OrcDataSource>> validationInputFactory;
    private long validationCpuNanos;

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
            OrcWriteValidationMode validationMode,
            OrcWriterStats stats)
    {
        requireNonNull(orcDataSink, "orcDataSink is null");
        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");
        this.fileInputColumnIndexes = requireNonNull(fileInputColumnIndexes, "fileInputColumnIndexes is null");

        this.nullBlocks = fileColumnTypes.stream()
                .map(type -> type.createBlockBuilder(null, 1, 0).appendNull().build())
                .collect(toImmutableList());

        this.validationInputFactory = validationInputFactory;
        this.orcWriter = new OrcWriter(
                orcDataSink,
                columnNames,
                fileColumnTypes,
                fileColumnOrcTypes,
                compression,
                options,
                metadata,
                validationInputFactory.isPresent(),
                validationMode,
                stats);
        this.icebergSchema = requireNonNull(icebergSchema, "icebergSchema is null");
        this.metricsConfig = requireNonNull(metricsConfig, "metricsConfig is null");
        orcColumns = fileColumnOrcTypes;
    }

    @Override
    public Metrics getMetrics()
    {
        return computeMetrics(metricsConfig, icebergSchema, orcColumns, orcWriter.getFileRowCount(), orcWriter.getFileStats());
    }

    @Override
    public long getWrittenBytes()
    {
        return orcWriter.getWrittenBytes() + orcWriter.getBufferedBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE + orcWriter.getRetainedBytes();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        Block[] blocks = new Block[fileInputColumnIndexes.length];
        for (int i = 0; i < fileInputColumnIndexes.length; i++) {
            int inputColumnIndex = fileInputColumnIndexes[i];
            if (inputColumnIndex < 0) {
                blocks[i] = RunLengthEncodedBlock.create(nullBlocks.get(i), dataPage.getPositionCount());
            }
            else {
                blocks[i] = dataPage.getBlock(inputColumnIndex);
            }
        }

        Page page = new Page(dataPage.getPositionCount(), blocks);
        try {
            orcWriter.write(page);
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public Closeable commit()
    {
        try {
            orcWriter.close();
        }
        catch (IOException | UncheckedIOException e) {
            try {
                rollbackAction.close();
            }
            catch (IOException | RuntimeException ex) {
                if (!e.equals(ex)) {
                    e.addSuppressed(ex);
                }
                log.error(ex, "Exception when committing file");
            }
            throw new TrinoException(ICEBERG_WRITER_CLOSE_ERROR, "Error committing write to ORC file", e);
        }

        if (validationInputFactory.isPresent()) {
            try {
                try (OrcDataSource input = validationInputFactory.get().get()) {
                    long startThreadCpuTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
                    orcWriter.validate(input);
                    validationCpuNanos += THREAD_MX_BEAN.getCurrentThreadCpuTime() - startThreadCpuTime;
                }
            }
            catch (IOException | UncheckedIOException e) {
                throw new TrinoException(ICEBERG_WRITE_VALIDATION_FAILED, e);
            }
        }

        return rollbackAction;
    }

    @Override
    public void rollback()
    {
        try (rollbackAction) {
            orcWriter.close();
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_WRITER_CLOSE_ERROR, "Error rolling back write to ORC file", e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return validationCpuNanos;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("writer", orcWriter)
                .toString();
    }
}
