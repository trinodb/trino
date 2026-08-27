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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.orc.MemoryOrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.format.orc.OrcFileFormatFactory;
import org.apache.paimon.format.parquet.ParquetFileFormatFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FormatReaderMapping;
import org.apache.paimon.utils.RoaringBitmap32;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoPaimonFileFormatProvider
{
    private static final String UNSUPPORTED_FORMAT_READ_MESSAGE = "Trino Paimon file format does not support Paimon BLOB, VARIANT, VECTOR, or MULTISET reads";
    private static final String UNSUPPORTED_FORMAT_WRITE_MESSAGE = "Trino Paimon file format does not support Paimon BLOB, VARIANT, VECTOR, or MULTISET writes";

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void testPaimonDiscoversTrinoFactoriesForBuiltInFormatIdentifiers()
    {
        assertThat(FileFormat.fromIdentifier("parquet", trinoFormatOptions()))
                .isInstanceOf(TrinoPaimonFileFormat.class);
        assertThat(FileFormat.fromIdentifier("orc", trinoFormatOptions()))
                .isInstanceOf(TrinoPaimonFileFormat.class);
    }

    @Test
    void testParquetWriterRoundTripWithPaimonReader()
            throws Exception
    {
        assertRoundTrip("parquet", "snappy");
    }

    @Test
    void testOrcWriterRoundTripWithPaimonReader()
            throws Exception
    {
        assertRoundTrip("orc", "zstd");
    }

    @Test
    void testWriterCloseLeavesPaimonOutputStreamOpen()
            throws Exception
    {
        assertWriterCloseLeavesPaimonOutputStreamOpen("parquet", "snappy");
        assertWriterCloseLeavesPaimonOutputStreamOpen("orc", "zstd");
    }

    @Test
    void testWriterCloseReleasesAdapterAfterFlushFailure()
            throws Exception
    {
        assertWriterCloseReleasesAdapterAfterFlushFailure("parquet", "snappy");
        assertWriterCloseReleasesAdapterAfterFlushFailure("orc", "zstd");
    }

    @Test
    void testWriterMetadataPreservesPaimonColumnStats()
            throws Exception
    {
        assertWriterMetadataPreservesPaimonColumnStats("parquet", "snappy");
        assertWriterMetadataPreservesPaimonColumnStats("orc", "zstd");
    }

    @Test
    void testStatsExtractorRejectsMismatchedWriterMetadataAsIoFailure()
            throws Exception
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()));
        Path file = new Path(tempDir.resolve("stats-mismatch.parquet").toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();
        FileFormat trinoFormat = FileFormat.fromIdentifier("parquet", trinoFormatOptions());
        Object writerMetadata;
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = trinoFormat.createWriterFactory(rowType).create(out, "snappy");
            writer.addElement(GenericRow.of(1, BinaryString.fromString("alpha")));
            writer.close();
            writerMetadata = writer.writerMetadata();
        }

        SimpleStatsExtractor mismatchedStatsExtractor = trinoFormat.createStatsExtractor(
                        DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                        new SimpleColStatsCollector.Factory[] {
                                SimpleColStatsCollector.from("FULL"),
                        })
                .orElseThrow();

        assertThatThrownBy(() -> mismatchedStatsExtractor.extract(fileIO, file, 0, writerMetadata))
                .isInstanceOf(IOException.class)
                .hasMessage("Trino Paimon writer metadata column stats count 2 does not match stats collector count 1");
    }

    @Test
    void testPaimonTableWriteStoresFormatColumnStatsInDataFileMeta()
            throws Exception
    {
        assertPaimonTableWriteStoresFormatColumnStatsInDataFileMeta("parquet", "snappy");
        assertPaimonTableWriteStoresFormatColumnStatsInDataFileMeta("orc", "zstd");
    }

    @Test
    void testTrinoReaderPreservesFilePositionsForPaimonSelection()
            throws Exception
    {
        assertTrinoReaderPreservesFilePositionsForPaimonSelection("parquet", "snappy");
        assertTrinoReaderPreservesFilePositionsForPaimonSelection("orc", "zstd");
    }

    @Test
    void testTrinoOrcReaderReusesPaimonInputStream()
            throws Exception
    {
        RowType rowType = rowType();
        List<GenericRow> rows = rows();
        Path file = new Path(tempDir.resolve("stream-reuse-data.orc").toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();
        FileFormat trinoWriteFormat = FileFormat.fromIdentifier("orc", trinoFormatOptions());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoWriteFormat.createWriterFactory(rowType).create(out, "zstd")) {
            for (GenericRow row : rows) {
                writer.addElement(row);
            }
        }

        CountingFileIO countingFileIO = new CountingFileIO();
        FileFormat trinoReadFormat = FileFormat.fromIdentifier("orc", trinoFormatOptions());

        assertThat(readRows(trinoReadFormat, rowType, countingFileIO, file))
                .containsExactlyElementsOf(canonicalizeRows(rowType, rows));
        assertThat(countingFileIO.newInputStreamCount()).isEqualTo(1);
        assertThat(countingFileIO.inputStreamCloseCount()).isEqualTo(1);
    }

    @Test
    void testTrinoReaderWorksWithPaimonSchemaEvolutionMapping()
            throws Exception
    {
        assertTrinoReaderWorksWithPaimonSchemaEvolutionMapping("parquet", "snappy");
        assertTrinoReaderWorksWithPaimonSchemaEvolutionMapping("orc", "zstd");
    }

    @Test
    void testParquetWriterUsesPaimonFileBlockSize()
            throws Exception
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "payload", DataTypes.STRING()));
        Path file = new Path(tempDir.resolve("block-size-data.parquet").toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();
        FileFormat trinoWriteFormat = FileFormat.fromIdentifier("parquet", trinoFormatOptionsWithBlockSize(2 * 1024));
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoWriteFormat.createWriterFactory(rowType).create(out, "snappy")) {
            for (GenericRow row : largeRows(200)) {
                writer.addElement(row);
            }
        }

        Slice data = Slices.wrappedBuffer(Files.readAllBytes(java.nio.file.Path.of(file.toUri())));
        ParquetMetadata metadata = MetadataReader.readFooter(
                new SliceParquetDataSource(data, ParquetReaderOptions.defaultOptions()),
                Optional.empty());
        assertThat(metadata.getBlocks())
                .hasSizeGreaterThan(1)
                .allSatisfy(block -> assertThat(block.rowCount()).isPositive());
    }

    @Test
    void testParquetWriterUsesPaimonParquetPageOptions()
            throws Exception
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "payload", DataTypes.STRING()));
        Path file = new Path(tempDir.resolve("page-options-data.parquet").toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();
        Options options = trinoFormatOptions();
        options.set("parquet.block.size", String.valueOf(1024 * 1024));
        options.set("parquet.page.size", "1024");
        options.set("parquet.page.row.count.limit", "5");
        FileFormat trinoWriteFormat = FileFormat.fromIdentifier("parquet", options);
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoWriteFormat.createWriterFactory(rowType).create(out, "snappy")) {
            for (GenericRow row : largeRows(40)) {
                writer.addElement(row);
            }
        }

        Slice data = Slices.wrappedBuffer(Files.readAllBytes(java.nio.file.Path.of(file.toUri())));
        ParquetMetadata metadata = MetadataReader.readFooter(
                new SliceParquetDataSource(data, ParquetReaderOptions.defaultOptions()),
                Optional.empty());
        assertThat(metadata.getBlocks()).hasSize(1);

        assertThat(readDataPageValueCounts(data, metadata.getBlocks().get(0).columns().get(0)))
                .hasSizeGreaterThan(1)
                .allSatisfy(valueCount -> assertThat(valueCount).isLessThanOrEqualTo(5));
    }

    @Test
    void testOrcWriterUsesPaimonFileBlockSize()
            throws Exception
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "payload", DataTypes.STRING()));
        Path file = new Path(tempDir.resolve("block-size-data.orc").toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();
        FileFormat trinoWriteFormat = FileFormat.fromIdentifier("orc", trinoFormatOptionsWithBlockSize(2 * 1024));
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoWriteFormat.createWriterFactory(rowType).create(out, "zstd")) {
            for (GenericRow row : largeRows(200)) {
                writer.addElement(row);
            }
        }

        Slice data = Slices.wrappedBuffer(Files.readAllBytes(java.nio.file.Path.of(file.toUri())));
        assertThat(OrcReader.createOrcReader(
                        new MemoryOrcDataSource(new OrcDataSourceId(file.toString()), data),
                        new OrcReaderOptions())
                .orElseThrow()
                .getFooter()
                .getStripes())
                .hasSizeGreaterThan(1)
                .allSatisfy(stripe -> assertThat(stripe.getNumberOfRows()).isPositive());
    }

    @Test
    void testWriterRejectsNonPositivePaimonFileBlockSize()
    {
        FileFormat trinoWriteFormat = FileFormat.fromIdentifier("parquet", trinoFormatOptionsWithBlockSize(0));

        assertThatThrownBy(() -> trinoWriteFormat.createWriterFactory(rowType()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("file.block-size must be greater than 0 bytes");
    }

    @Test
    void testWriterRejectsNonPositivePaimonParquetPageOptions()
    {
        Options pageSizeOptions = trinoFormatOptions();
        pageSizeOptions.set("parquet.page.size", "0");
        FileFormat pageSizeFormat = FileFormat.fromIdentifier("parquet", pageSizeOptions);

        assertThatThrownBy(() -> pageSizeFormat.createWriterFactory(rowType()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("parquet.page.size must be greater than 0");

        Options rowCountLimitOptions = trinoFormatOptions();
        rowCountLimitOptions.set("parquet.page.row.count.limit", "0");
        FileFormat rowCountLimitFormat = FileFormat.fromIdentifier("parquet", rowCountLimitOptions);

        assertThatThrownBy(() -> rowCountLimitFormat.createWriterFactory(rowType()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("parquet.page.row.count.limit must be greater than 0");
    }

    @Test
    void testTrinoReaderRejectsPaimonSpecialTypes()
    {
        FileFormat trinoReadFormat = FileFormat.fromIdentifier("parquet", trinoFormatOptions());

        for (RowType rowType : unsupportedTrinoFormatTypes()) {
            assertThatThrownBy(() -> trinoReadFormat.createReaderFactory(rowType, rowType, new ArrayList<>()))
                    .as("read format should reject %s", rowType)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage(UNSUPPORTED_FORMAT_READ_MESSAGE);
        }
    }

    @Test
    void testTrinoWriterRejectsPaimonSpecialTypes()
    {
        FileFormat trinoWriteFormat = FileFormat.fromIdentifier("parquet", trinoFormatOptions());

        for (RowType rowType : unsupportedTrinoFormatTypes()) {
            assertThatThrownBy(() -> trinoWriteFormat.createWriterFactory(rowType))
                    .as("write format should reject %s", rowType)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage(UNSUPPORTED_FORMAT_WRITE_MESSAGE);
        }
    }

    @Test
    void testTrinoOrcWriterRejectsTimeColumnsWithActionableMessage()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "event_time", DataTypes.TIME(3)),
                DataTypes.FIELD(2, "nested", DataTypes.ROW(
                        DataTypes.FIELD(3, "nested_time", DataTypes.TIME(3)))));

        assertThatThrownBy(() -> FileFormat.fromIdentifier("orc", trinoFormatOptions()).createWriterFactory(rowType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Trino Paimon ORC writer does not support Paimon TIME columns; use Parquet or Paimon's native writer for ORC TIME data");
        assertThatCode(() -> FileFormat.fromIdentifier("parquet", trinoFormatOptions()).createWriterFactory(rowType))
                .doesNotThrowAnyException();
    }

    @Test
    void testValidationDoesNotRejectTypesUnsupportedOnlyByTrinoWriterSchemaConverters()
    {
        RowType timestampWithTimeZoneRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "event_time", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)));
        RowType variantRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));

        assertThatCode(() -> FileFormat.fromIdentifier("parquet", trinoFormatOptions()).validateDataFields(timestampWithTimeZoneRowType))
                .doesNotThrowAnyException();
        assertThatCode(() -> FileFormat.fromIdentifier("orc", trinoFormatOptions()).validateDataFields(timestampWithTimeZoneRowType))
                .doesNotThrowAnyException();
        assertThatCode(() -> FileFormat.fromIdentifier("parquet", trinoFormatOptions()).validateDataFields(variantRowType))
                .doesNotThrowAnyException();
        assertThatCode(() -> FileFormat.fromIdentifier("orc", trinoFormatOptions()).validateDataFields(variantRowType))
                .doesNotThrowAnyException();
    }

    private void assertRoundTrip(String formatIdentifier, String compression)
            throws Exception
    {
        RowType rowType = rowType();
        List<GenericRow> rows = rows();
        Path file = new Path(tempDir.resolve("data." + formatIdentifier).toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();

        FileFormat trinoFormat = FileFormat.fromIdentifier(formatIdentifier, trinoFormatOptions());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoFormat.createWriterFactory(rowType).create(out, compression)) {
            for (GenericRow row : rows) {
                writer.addElement(row);
            }
        }

        FileFormat paimonFormat = paimonFileFormat(formatIdentifier);
        assertThat(readRows(paimonFormat, rowType, fileIO, file))
                .containsExactlyElementsOf(canonicalizeRows(rowType, rows));

        FileFormat trinoReadFormat = FileFormat.fromIdentifier(formatIdentifier, trinoFormatOptions());
        assertThat(readRows(trinoReadFormat, rowType, fileIO, file))
                .containsExactlyElementsOf(canonicalizeRows(rowType, rows));
    }

    private static void assertWriterCloseLeavesPaimonOutputStreamOpen(String formatIdentifier, String compression)
            throws IOException
    {
        TrackingPositionOutputStream out = new TrackingPositionOutputStream();
        FileFormat trinoFormat = FileFormat.fromIdentifier(formatIdentifier, trinoFormatOptions());

        trinoFormat.createWriterFactory(rowType()).create(out, compression).close();

        assertThat(out.closed()).isFalse();
        assertThatCode(out::flush).doesNotThrowAnyException();
        assertThatCode(out::close).doesNotThrowAnyException();
        assertThat(out.closed()).isTrue();
    }

    private static void assertWriterCloseReleasesAdapterAfterFlushFailure(String formatIdentifier, String compression)
            throws Exception
    {
        TrackingPositionOutputStream out = new TrackingPositionOutputStream();
        Options options = new Options();
        options.set(CoreOptions.WRITE_BATCH_SIZE, 10);
        FileFormat trinoFormat = FileFormat.fromIdentifier(formatIdentifier, options);
        FormatWriter writer = trinoFormat.createWriterFactory(rowType()).create(out, compression);
        FailingWriterAdapter failingWriterAdapter = installFailingWriterAdapter(writer);

        writer.addElement(rows().get(0));

        assertThatThrownBy(writer::close)
                .isInstanceOf(IOException.class)
                .hasMessage("flush failed");
        assertThat(failingWriterAdapter.writeCount()).isEqualTo(1);
        assertThat(failingWriterAdapter.closeCount()).isEqualTo(1);
        assertThat(writer.writerMetadata()).isNull();
    }

    private static FailingWriterAdapter installFailingWriterAdapter(FormatWriter writer)
            throws ReflectiveOperationException
    {
        Field writerField = TrinoPaimonFormatWriter.class.getDeclaredField("writer");
        writerField.setAccessible(true);
        FailingWriterAdapter failingWriterAdapter = new FailingWriterAdapter();
        Object proxy = Proxy.newProxyInstance(
                writerField.getType().getClassLoader(),
                new Class<?>[] {writerField.getType()},
                failingWriterAdapter);
        writerField.set(writer, proxy);
        return failingWriterAdapter;
    }

    private void assertWriterMetadataPreservesPaimonColumnStats(String formatIdentifier, String compression)
            throws Exception
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()));
        Path file = new Path(tempDir.resolve("stats-data." + formatIdentifier).toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();
        FileFormat trinoFormat = FileFormat.fromIdentifier(formatIdentifier, trinoFormatOptions());

        Object writerMetadata;
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = trinoFormat.createWriterFactory(rowType).create(out, compression);
            writer.addElement(GenericRow.of(2, BinaryString.fromString("beta")));
            writer.addElement(GenericRow.of(null, BinaryString.fromString("alpha")));
            writer.addElement(GenericRow.of(1, null));

            assertThat(writer.writerMetadata()).isNull();
            writer.close();
            writerMetadata = writer.writerMetadata();
        }

        assertThat(writerMetadata).isNotNull();

        SimpleStatsExtractor fullStatsExtractor = trinoFormat.createStatsExtractor(
                        rowType,
                        new SimpleColStatsCollector.Factory[] {
                                SimpleColStatsCollector.from("FULL"),
                                SimpleColStatsCollector.from("FULL"),
                        })
                .orElseThrow();
        SimpleColStats[] fullStats = fullStatsExtractor.extract(fileIO, file, 0, writerMetadata);

        assertThat(fullStats[0].min()).isEqualTo(1);
        assertThat(fullStats[0].max()).isEqualTo(2);
        assertThat(fullStats[0].nullCount()).isEqualTo(1L);
        assertThat(fullStats[1].min()).isEqualTo(BinaryString.fromString("alpha"));
        assertThat(fullStats[1].max()).isEqualTo(BinaryString.fromString("beta"));
        assertThat(fullStats[1].nullCount()).isEqualTo(1L);

        SimpleStatsExtractor countsStatsExtractor = trinoFormat.createStatsExtractor(
                        rowType,
                        new SimpleColStatsCollector.Factory[] {
                                SimpleColStatsCollector.from("COUNTS"),
                                SimpleColStatsCollector.from("COUNTS"),
                        })
                .orElseThrow();
        SimpleColStats[] countsStats = countsStatsExtractor.extract(fileIO, file, 0, writerMetadata);

        assertThat(countsStats[0].min()).isNull();
        assertThat(countsStats[0].max()).isNull();
        assertThat(countsStats[0].nullCount()).isEqualTo(1L);
        assertThat(countsStats[1].min()).isNull();
        assertThat(countsStats[1].max()).isNull();
        assertThat(countsStats[1].nullCount()).isEqualTo(1L);

        assertThatThrownBy(() -> fullStatsExtractor.extract(fileIO, file, fileIO.getFileSize(file)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Trino Paimon file format can extract column stats only from writer metadata");
    }

    private void assertPaimonTableWriteStoresFormatColumnStatsInDataFileMeta(String formatIdentifier, String compression)
            throws Exception
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()));
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.resolve("stats-table-" + formatIdentifier).toUri().toString());
        Options options = trinoFormatOptions();
        options.set(CoreOptions.FILE_FORMAT, formatIdentifier);
        options.set(CoreOptions.FILE_COMPRESSION, compression);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.BUCKET_KEY, "id");
        options.set(CoreOptions.METADATA_STATS_MODE, "full");
        new SchemaManager(fileIO, tablePath).createTable(new Schema(
                rowType.getFields(),
                Collections.emptyList(),
                Collections.emptyList(),
                options.toMap(),
                ""));

        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        List<CommitMessage> commitMessages;
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            write.write(GenericRow.of(2, BinaryString.fromString("beta")));
            write.write(GenericRow.of(null, BinaryString.fromString("alpha")));
            write.write(GenericRow.of(1, null));
            commitMessages = write.prepareCommit();
        }

        List<DataFileMeta> dataFiles = commitMessages.stream()
                .map(CommitMessageImpl.class::cast)
                .flatMap(message -> message.newFilesIncrement().newFiles().stream())
                .toList();
        assertThat(dataFiles).hasSize(1);
        DataFileMeta dataFile = dataFiles.get(0);
        assertThat(dataFile.rowCount()).isEqualTo(3);
        assertThat(dataFile.valueStatsCols()).isNull();

        SimpleStats stats = dataFile.valueStats();
        assertThat(stats.minValues().getInt(0)).isEqualTo(1);
        assertThat(stats.maxValues().getInt(0)).isEqualTo(2);
        assertThat(stats.nullCounts().getLong(0)).isEqualTo(1L);
        assertThat(stats.minValues().getString(1)).isEqualTo(BinaryString.fromString("alpha"));
        assertThat(stats.maxValues().getString(1)).isEqualTo(BinaryString.fromString("beta"));
        assertThat(stats.nullCounts().getLong(1)).isEqualTo(1L);

        try (BatchTableCommit commit = writeBuilder.newCommit()) {
            commit.commit(commitMessages);
        }
    }

    private void assertTrinoReaderPreservesFilePositionsForPaimonSelection(String formatIdentifier, String compression)
            throws Exception
    {
        RowType rowType = rowType();
        List<GenericRow> rows = rows();
        List<InternalRow> canonicalRows = canonicalizeRows(rowType, rows);
        Path file = new Path(tempDir.resolve("selection-data." + formatIdentifier).toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();
        FileFormat trinoWriteFormat = FileFormat.fromIdentifier(formatIdentifier, trinoFormatOptions());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoWriteFormat.createWriterFactory(rowType).create(out, compression)) {
            for (GenericRow row : rows) {
                writer.addElement(row);
            }
        }

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(1);
        selection.add(2);
        FileFormat trinoReadFormat = FileFormat.fromIdentifier(formatIdentifier, trinoFormatOptions());

        assertThat(readRowsWithPositions(trinoReadFormat, rowType, fileIO, file, selection))
                .containsExactly(
                        new PositionedRow(1, canonicalRows.get(1)),
                        new PositionedRow(2, canonicalRows.get(2)));
    }

    private void assertTrinoReaderWorksWithPaimonSchemaEvolutionMapping(String formatIdentifier, String compression)
            throws Exception
    {
        TableSchema dataSchema = tableSchema(
                1,
                DataTypes.ROW(
                        DataTypes.FIELD(0, "old_name", DataTypes.STRING()),
                        DataTypes.FIELD(1, "old_amount", DataTypes.INT())));
        TableSchema tableSchema = tableSchema(
                2,
                DataTypes.ROW(
                        DataTypes.FIELD(1, "amount", DataTypes.BIGINT()),
                        DataTypes.FIELD(2, "new_comment", DataTypes.STRING()),
                        DataTypes.FIELD(0, "name", DataTypes.STRING())));
        RowType tableRowType = tableSchema.logicalRowType();
        Path file = new Path(tempDir.resolve("schema-evolution-data." + formatIdentifier).toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();

        FileFormat trinoWriteFormat = FileFormat.fromIdentifier(formatIdentifier, trinoFormatOptions());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoWriteFormat.createWriterFactory(dataSchema.logicalRowType()).create(out, compression)) {
            writer.addElement(GenericRow.of(BinaryString.fromString("alpha"), 12));
            writer.addElement(GenericRow.of(BinaryString.fromString("beta"), 34));
        }

        FormatReaderMapping mapping = new FormatReaderMapping.Builder(
                identifier -> FileFormat.fromIdentifier(identifier, trinoFormatOptions()),
                tableSchema.fields(),
                TableSchema::fields,
                new ArrayList<>(),
                null,
                null)
                .build(formatIdentifier, tableSchema, dataSchema);
        InternalRowSerializer serializer = new InternalRowSerializer(tableRowType);

        try (FileRecordReader<InternalRow> reader = new DataFileRecordReader(
                tableRowType,
                mapping.getReaderFactory(),
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)),
                false,
                false,
                mapping.getIndexMapping(),
                mapping.getCastMapping(),
                null,
                false,
                null,
                0,
                mapping.getSystemFields())) {
            List<InternalRow> rows = new ArrayList<>();
            reader.forEachRemaining(row -> rows.add(serializer.toBinaryRow(row).copy()));

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getLong(0)).isEqualTo(12L);
            assertThat(rows.get(0).isNullAt(1)).isTrue();
            assertThat(rows.get(0).getString(2)).isEqualTo(BinaryString.fromString("alpha"));
            assertThat(rows.get(1).getLong(0)).isEqualTo(34L);
            assertThat(rows.get(1).isNullAt(1)).isTrue();
            assertThat(rows.get(1).getString(2)).isEqualTo(BinaryString.fromString("beta"));
        }

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(1);
        try (FileRecordReader<InternalRow> reader = new DataFileRecordReader(
                tableRowType,
                mapping.getReaderFactory(),
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection),
                false,
                false,
                mapping.getIndexMapping(),
                mapping.getCastMapping(),
                null,
                false,
                null,
                0,
                mapping.getSystemFields())) {
            List<InternalRow> rows = new ArrayList<>();
            reader.forEachRemaining(row -> rows.add(serializer.toBinaryRow(row).copy()));

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong(0)).isEqualTo(34L);
            assertThat(rows.get(0).isNullAt(1)).isTrue();
            assertThat(rows.get(0).getString(2)).isEqualTo(BinaryString.fromString("beta"));
        }
    }

    private static Options trinoFormatOptions()
    {
        Options options = new Options();
        options.set(CoreOptions.WRITE_BATCH_SIZE, 1);
        return options;
    }

    private static Options trinoFormatOptionsWithBlockSize(long blockSizeBytes)
    {
        Options options = trinoFormatOptions();
        options.set(CoreOptions.FILE_BLOCK_SIZE, MemorySize.ofBytes(blockSizeBytes));
        return options;
    }

    private static FileFormat paimonFileFormat(String formatIdentifier)
    {
        FormatContext context = formatContext(new Options());
        return switch (formatIdentifier) {
            case "parquet" -> new ParquetFileFormatFactory().create(context);
            case "orc" -> new OrcFileFormatFactory().create(context);
            default -> throw new IllegalArgumentException("Unsupported Paimon file format: " + formatIdentifier);
        };
    }

    private static FormatContext formatContext(Options options)
    {
        return new FormatContext(
                options,
                options.get(CoreOptions.READ_BATCH_SIZE),
                options.get(CoreOptions.WRITE_BATCH_SIZE),
                options.get(CoreOptions.WRITE_BATCH_MEMORY),
                options.get(CoreOptions.FILE_COMPRESSION_ZSTD_LEVEL),
                options.get(CoreOptions.FILE_BLOCK_SIZE));
    }

    private static RowType rowType()
    {
        return DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()),
                DataTypes.FIELD(2, "amount", DataTypes.DECIMAL(12, 2)),
                DataTypes.FIELD(3, "created_at", DataTypes.TIMESTAMP(6)),
                DataTypes.FIELD(4, "scores", DataTypes.ARRAY(DataTypes.INT())),
                DataTypes.FIELD(5, "attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                DataTypes.FIELD(
                        6,
                        "payload",
                        DataTypes.ROW(
                                DataTypes.FIELD(7, "flag", DataTypes.BOOLEAN()),
                                DataTypes.FIELD(8, "note", DataTypes.STRING()))));
    }

    private static List<RowType> unsupportedTrinoFormatTypes()
    {
        return List.of(
                rowTypeWith(DataTypes.BLOB()),
                rowTypeWith(DataTypes.VARIANT()),
                rowTypeWith(DataTypes.VECTOR(3, DataTypes.FLOAT())),
                rowTypeWith(DataTypes.MULTISET(DataTypes.STRING())),
                rowTypeWith(DataTypes.ARRAY(DataTypes.VARIANT())),
                rowTypeWith(DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB())),
                rowTypeWith(
                        DataTypes.ROW(
                                DataTypes.FIELD(10, "nested_vector", DataTypes.VECTOR(3, DataTypes.FLOAT())))));
    }

    private static RowType rowTypeWith(DataType type)
    {
        return DataTypes.ROW(DataTypes.FIELD(0, "payload", type));
    }

    private static TableSchema tableSchema(long id, RowType rowType)
    {
        return new TableSchema(
                id,
                rowType.getFields(),
                rowType.getFields().stream()
                        .mapToInt(DataField::id)
                        .max()
                        .orElse(0),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
    }

    private static List<GenericRow> rows()
    {
        return List.of(
                GenericRow.of(
                        1,
                        BinaryString.fromString("alpha"),
                        Decimal.fromBigDecimal(new BigDecimal("12.34"), 12, 2),
                        Timestamp.fromEpochMillis(1_695_645_403_123L, 456_000),
                        new GenericArray(new int[] {1, 2, 3}),
                        new GenericMap(
                                Map.of(
                                        BinaryString.fromString("red"), 7,
                                        BinaryString.fromString("blue"), 11)),
                        GenericRow.of(true, BinaryString.fromString("nested-alpha"))),
                GenericRow.of(
                        2,
                        BinaryString.fromString("beta"),
                        Decimal.fromBigDecimal(new BigDecimal("56.78"), 12, 2),
                        Timestamp.fromEpochMillis(1_695_645_404_000L, 0),
                        new GenericArray(new int[] {4, 5}),
                        new GenericMap(Map.of(BinaryString.fromString("green"), 13)),
                        GenericRow.of(false, BinaryString.fromString("nested-beta"))),
                GenericRow.of(
                        3,
                        BinaryString.fromString("gamma"),
                        Decimal.fromBigDecimal(new BigDecimal("90.12"), 12, 2),
                        Timestamp.fromEpochMillis(1_695_645_405_000L, 123_000),
                        new GenericArray(new int[] {6}),
                        new GenericMap(Map.of(BinaryString.fromString("yellow"), 17)),
                        GenericRow.of(false, BinaryString.fromString("nested-beta"))));
    }

    private static List<GenericRow> largeRows(int count)
    {
        String payload = "x".repeat(1024);
        List<GenericRow> rows = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            rows.add(GenericRow.of(index, BinaryString.fromString(payload + index)));
        }
        return rows;
    }

    private static List<Integer> readDataPageValueCounts(Slice fileData, ColumnChunkMetadata columnChunk)
            throws IOException
    {
        long start = columnChunk.getStartingPos();
        long end = start + columnChunk.getTotalSize();
        ByteArrayInputStream input = new ByteArrayInputStream(fileData.getBytes((int) start, (int) (end - start)));
        List<Integer> valueCounts = new ArrayList<>();
        while (input.available() > 0) {
            PageHeader pageHeader = Util.readPageHeader(input);
            if (pageHeader.isSetData_page_header()) {
                valueCounts.add(pageHeader.getData_page_header().getNum_values());
            }
            if (pageHeader.isSetData_page_header_v2()) {
                valueCounts.add(pageHeader.getData_page_header_v2().getNum_values());
            }
            input.skipNBytes(pageHeader.getCompressed_page_size());
        }
        return valueCounts;
    }

    private static List<InternalRow> readRows(
            FileFormat format,
            RowType rowType,
            LocalFileIO fileIO,
            Path file)
            throws IOException
    {
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        List<InternalRow> rows = new ArrayList<>();
        try (FileRecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            reader.forEachRemaining(row -> rows.add(serializer.toBinaryRow(row).copy()));
        }
        return rows;
    }

    private static List<PositionedRow> readRowsWithPositions(
            FileFormat format,
            RowType rowType,
            LocalFileIO fileIO,
            Path file,
            RoaringBitmap32 selection)
            throws IOException
    {
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        List<PositionedRow> rows = new ArrayList<>();
        try (FileRecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection))) {
            reader.forEachRemainingWithPosition(
                    (position, row) ->
                            rows.add(new PositionedRow(position, serializer.toBinaryRow(row).copy())));
        }
        return rows;
    }

    private static List<InternalRow> canonicalizeRows(RowType rowType, List<GenericRow> rows)
    {
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        return rows.stream()
                .map(row -> (InternalRow) serializer.toBinaryRow(row).copy())
                .toList();
    }

    private record PositionedRow(long position, InternalRow row) {}

    private static class CountingFileIO
            extends LocalFileIO
    {
        private int newInputStreamCount;
        private int inputStreamCloseCount;

        @Override
        public SeekableInputStream newInputStream(Path path)
                throws IOException
        {
            newInputStreamCount++;
            return new CountingSeekableInputStream(super.newInputStream(path));
        }

        int newInputStreamCount()
        {
            return newInputStreamCount;
        }

        int inputStreamCloseCount()
        {
            return inputStreamCloseCount;
        }

        private class CountingSeekableInputStream
                extends SeekableInputStream
        {
            private final SeekableInputStream delegate;

            private CountingSeekableInputStream(SeekableInputStream delegate)
            {
                this.delegate = requireNonNull(delegate, "delegate is null");
            }

            @Override
            public void seek(long desired)
                    throws IOException
            {
                delegate.seek(desired);
            }

            @Override
            public long getPos()
                    throws IOException
            {
                return delegate.getPos();
            }

            @Override
            public int read()
                    throws IOException
            {
                return delegate.read();
            }

            @Override
            public int read(byte[] buffer, int offset, int length)
                    throws IOException
            {
                return delegate.read(buffer, offset, length);
            }

            @Override
            public void close()
                    throws IOException
            {
                inputStreamCloseCount++;
                delegate.close();
            }
        }
    }

    private static class TrackingPositionOutputStream
            extends PositionOutputStream
    {
        private long position;
        private boolean closed;

        @Override
        public long getPos()
        {
            return position;
        }

        @Override
        public void write(int b)
        {
            position++;
        }

        @Override
        public void write(byte[] b)
                throws IOException
        {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len)
        {
            position += len;
        }

        @Override
        public void flush()
                throws IOException
        {
            if (closed) {
                throw new IOException("Already closed");
            }
        }

        @Override
        public void close()
        {
            closed = true;
        }

        boolean closed()
        {
            return closed;
        }
    }

    private static class FailingWriterAdapter
            implements InvocationHandler
    {
        private int writeCount;
        private int closeCount;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable
        {
            return switch (method.getName()) {
                case "write" -> {
                    writeCount++;
                    throw new IOException("flush failed");
                }
                case "close" -> {
                    closeCount++;
                    yield null;
                }
                case "getWrittenBytes", "getBufferedBytes" -> 0L;
                case "toString" -> "FailingWriterAdapter";
                default -> throw new UnsupportedOperationException("Unexpected method: " + method);
            };
        }

        int writeCount()
        {
            return writeCount;
        }

        int closeCount()
        {
            return closeCount;
        }
    }

    private static class SliceParquetDataSource
            extends AbstractParquetDataSource
    {
        private final Slice data;

        private SliceParquetDataSource(Slice data, ParquetReaderOptions options)
        {
            super(new ParquetDataSourceId("slice"), data.length(), options);
            this.data = data;
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
        {
            data.getBytes((int) position, buffer, bufferOffset, bufferLength);
        }
    }
}
