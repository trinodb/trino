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
package io.trino.plugin.paimon.format;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcWriter;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.OutputStreamOrcDataSink;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcType;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.paimon.PaimonPageBuilder;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.fs.CloseShieldOutputStream;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.parquet.format.CompressionCodec;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.trino.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static io.trino.plugin.paimon.format.TrinoPaimonFileFormat.ORC;
import static io.trino.plugin.paimon.format.TrinoPaimonFileFormat.PARQUET;
import static java.util.Objects.requireNonNull;

class TrinoPaimonFormatWriter
        implements FormatWriter
{
    private static final String TRINO_PAIMON_WRITER_VERSION = "trino-paimon";
    private static final String TRINO_PAIMON_WRITER_METADATA_KEY = "trino.paimon.writer";

    private final PaimonPageBuilder pageBuilder;
    private final SimpleStatsCollector statsCollector;
    private final int writeBatchSize;
    private final WriterAdapter writer;
    private boolean closed;
    private SimpleColStats[] stats;

    TrinoPaimonFormatWriter(
            String formatIdentifier,
            RowType rowType,
            List<String> columnNames,
            List<Type> columnTypes,
            List<DataType> logicalTypes,
            int writeBatchSize,
            TrinoPaimonFormatWriterOptions writerOptions,
            PositionOutputStream out,
            String compression)
            throws IOException
    {
        this.statsCollector = new SimpleStatsCollector(requireNonNull(rowType, "rowType is null"));
        this.pageBuilder = new PaimonPageBuilder(columnTypes, logicalTypes);
        this.writeBatchSize = writeBatchSize;
        this.writer = switch (requireNonNull(formatIdentifier, "formatIdentifier is null")) {
            case PARQUET -> createParquetWriter(out, columnNames, columnTypes, writerOptions, compression);
            case ORC -> createOrcWriter(out, columnNames, columnTypes, writerOptions, compression);
            default -> throw new UnsupportedOperationException(
                    "Unsupported Trino Paimon file format: " + formatIdentifier);
        };
    }

    @Override
    public void addElement(InternalRow element)
            throws IOException
    {
        pageBuilder.appendRow(element);
        if (pageBuilder.isFull()
                || (writeBatchSize > 0 && pageBuilder.getPositionCount() >= writeBatchSize)) {
            flush();
        }
        statsCollector.collect(element);
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize)
    {
        return suggestedCheck && writer.getWrittenBytes() + writer.getBufferedBytes() >= targetSize;
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        boolean writerCloseAttempted = false;
        try {
            flush();
            writerCloseAttempted = true;
            writer.close();
            stats = statsCollector.extract();
        }
        catch (IOException | RuntimeException e) {
            if (!writerCloseAttempted) {
                try {
                    writer.close();
                }
                catch (IOException | RuntimeException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            throw e;
        }
        finally {
            closed = true;
        }
    }

    @Override
    public Object writerMetadata()
    {
        if (stats == null) {
            return null;
        }
        return new WriterMetadata(stats);
    }

    private void flush()
            throws IOException
    {
        if (pageBuilder.isEmpty()) {
            return;
        }
        Page page = pageBuilder.build();
        writer.write(page);
    }

    private static WriterAdapter createParquetWriter(
            PositionOutputStream out,
            List<String> columnNames,
            List<Type> columnTypes,
            TrinoPaimonFormatWriterOptions writerOptions,
            String compression)
    {
        ParquetSchemaConverter schemaConverter =
                new ParquetSchemaConverter(columnTypes, columnNames, true, true);
        ParquetWriterOptions.Builder parquetWriterOptions = ParquetWriterOptions.builder();
        writerOptions.blockSizeBytes().map(DataSize::ofBytes).ifPresent(parquetWriterOptions::setMaxBlockSize);
        writerOptions.parquetPageSizeBytes().map(DataSize::ofBytes).ifPresent(parquetWriterOptions::setMaxPageSize);
        writerOptions.parquetPageValueCountLimit().ifPresent(parquetWriterOptions::setMaxPageValueCount);
        ParquetWriter parquetWriter =
                new ParquetWriter(
                        new CloseShieldOutputStream(out),
                        schemaConverter.getMessageType(),
                        schemaConverter.getPrimitiveTypes(),
                        parquetWriterOptions.build(),
                        parquetCompressionCodec(compression),
                        TRINO_PAIMON_WRITER_VERSION,
                        Optional.of(DateTimeZone.UTC),
                        Optional.empty());
        return new ParquetWriterAdapter(parquetWriter);
    }

    private static WriterAdapter createOrcWriter(
            PositionOutputStream out,
            List<String> columnNames,
            List<Type> columnTypes,
            TrinoPaimonFormatWriterOptions writerOptions,
            String compression)
            throws IOException
    {
        OrcWriterOptions orcWriterOptions = new OrcWriterOptions();
        if (writerOptions.blockSizeBytes().isPresent()) {
            DataSize stripeSize = DataSize.ofBytes(writerOptions.blockSizeBytes().get());
            orcWriterOptions = orcWriterOptions
                    .withStripeMinSize(stripeSize)
                    .withStripeMaxSize(stripeSize);
        }
        OrcWriter orcWriter =
                new OrcWriter(
                        OutputStreamOrcDataSink.create(asTrinoOutputFile(out)),
                        columnNames,
                        columnTypes,
                        OrcType.createRootOrcType(columnNames, columnTypes),
                        orcCompressionKind(compression),
                        orcWriterOptions,
                        ImmutableMap.of(TRINO_PAIMON_WRITER_METADATA_KEY, TRINO_PAIMON_WRITER_VERSION),
                        true,
                        BOTH,
                        new OrcWriterStats());
        return new OrcWriterAdapter(orcWriter);
    }

    private static TrinoOutputFile asTrinoOutputFile(PositionOutputStream out)
    {
        requireNonNull(out, "out is null");
        return new TrinoOutputFile()
        {
            @Override
            public void createOrOverwrite(byte[] data)
                    throws IOException
            {
                out.write(data);
            }

            @Override
            public OutputStream create(AggregatedMemoryContext memoryContext)
            {
                return new CloseShieldOutputStream(out);
            }

            @Override
            public Location location()
            {
                return Location.of("memory:///paimon");
            }
        };
    }

    private static CompressionCodec parquetCompressionCodec(
            String compression)
    {
        return switch (normalizeCompression(compression)) {
            case "none", "uncompressed" -> CompressionCodec.UNCOMPRESSED;
            case "snappy" -> CompressionCodec.SNAPPY;
            case "gzip", "zlib" -> CompressionCodec.GZIP;
            case "lz4" -> CompressionCodec.LZ4;
            case "zstd" -> CompressionCodec.ZSTD;
            default -> throw new UnsupportedOperationException(
                    "Unsupported Parquet compression codec: " + compression);
        };
    }

    private static CompressionKind orcCompressionKind(String compression)
    {
        return switch (normalizeCompression(compression)) {
            case "none", "uncompressed" -> CompressionKind.NONE;
            case "snappy" -> CompressionKind.SNAPPY;
            case "gzip", "zlib" -> CompressionKind.ZLIB;
            case "lz4" -> CompressionKind.LZ4;
            case "zstd" -> CompressionKind.ZSTD;
            default -> throw new UnsupportedOperationException(
                    "Unsupported ORC compression codec: " + compression);
        };
    }

    private static String normalizeCompression(String compression)
    {
        return requireNonNull(compression, "compression is null").toLowerCase(Locale.ENGLISH);
    }

    private interface WriterAdapter
            extends Closeable
    {
        void write(Page page)
                throws IOException;

        long getWrittenBytes();

        long getBufferedBytes();
    }

    private record ParquetWriterAdapter(ParquetWriter writer)
            implements WriterAdapter
    {
        @Override
        public void write(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public long getWrittenBytes()
        {
            return writer.getEstimatedWrittenBytes();
        }

        @Override
        public long getBufferedBytes()
        {
            return writer.getRetainedBytes();
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }

    record WriterMetadata(SimpleColStats[] simpleColStats)
    {
        WriterMetadata
        {
            simpleColStats = Arrays.copyOf(requireNonNull(simpleColStats, "simpleColStats is null"), simpleColStats.length);
        }

        @Override
        public SimpleColStats[] simpleColStats()
        {
            return Arrays.copyOf(simpleColStats, simpleColStats.length);
        }
    }

    private record OrcWriterAdapter(OrcWriter writer)
            implements WriterAdapter
    {
        @Override
        public void write(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public long getWrittenBytes()
        {
            return writer.getWrittenBytes();
        }

        @Override
        public long getBufferedBytes()
        {
            return writer.getBufferedBytes();
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }
}
