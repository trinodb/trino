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
package io.trino.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.UnsignedBytes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.orc.OrcWriteValidation.OrcWriteValidationBuilder;
import io.trino.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.trino.orc.OrcWriterStats.FlushReason;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.CompressedMetadataWriter;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.Footer;
import io.trino.orc.metadata.Metadata;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcMetadataWriter;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.Stream;
import io.trino.orc.metadata.StripeFooter;
import io.trino.orc.metadata.StripeInformation;
import io.trino.orc.metadata.statistics.BloomFilterBuilder;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.metadata.statistics.NoOpBloomFilterBuilder;
import io.trino.orc.metadata.statistics.StripeStatistics;
import io.trino.orc.metadata.statistics.Utf8BloomFilterBuilder;
import io.trino.orc.stream.OrcDataOutput;
import io.trino.orc.stream.StreamDataOutput;
import io.trino.orc.writer.ColumnWriter;
import io.trino.orc.writer.SliceDictionaryColumnWriter;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.orc.OrcReader.validateFile;
import static io.trino.orc.OrcWriterStats.FlushReason.CLOSED;
import static io.trino.orc.OrcWriterStats.FlushReason.DICTIONARY_FULL;
import static io.trino.orc.OrcWriterStats.FlushReason.MAX_BYTES;
import static io.trino.orc.OrcWriterStats.FlushReason.MAX_ROWS;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static io.trino.orc.metadata.OrcColumnId.ROOT_COLUMN;
import static io.trino.orc.metadata.PostScript.MAGIC;
import static io.trino.orc.stream.OrcDataOutput.createDataOutput;
import static io.trino.orc.writer.ColumnWriters.createColumnWriter;
import static java.lang.Integer.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class OrcWriter
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcWriter.class).instanceSize();

    private static final String TRINO_ORC_WRITER_VERSION_METADATA_KEY = "trino.writer.version";
    private static final String TRINO_ORC_WRITER_VERSION;
    private final OrcWriterStats stats;

    static {
        String version = OrcWriter.class.getPackage().getImplementationVersion();
        TRINO_ORC_WRITER_VERSION = version == null ? "UNKNOWN" : version;
    }

    private final OrcDataSink orcDataSink;
    private final List<Type> types;
    private final CompressionKind compression;
    private final int stripeMaxBytes;
    private final int chunkMaxLogicalBytes;
    private final int stripeMaxRowCount;
    private final int rowGroupMaxRowCount;
    private final int maxCompressionBufferSize;
    private final Map<String, String> userMetadata = new HashMap<>();
    private final CompressedMetadataWriter metadataWriter;

    private final List<ClosedStripe> closedStripes = new ArrayList<>();
    private final ColumnMetadata<OrcType> orcTypes;

    private final List<ColumnWriter> columnWriters;
    private final DictionaryCompressionOptimizer dictionaryCompressionOptimizer;
    private int stripeRowCount;
    private int rowGroupRowCount;
    private int bufferedBytes;
    private long columnWritersRetainedBytes;
    private long closedStripesRetainedBytes;
    private long previouslyRecordedSizeInBytes;
    private boolean closed;

    private long fileRowCount;
    private Optional<ColumnMetadata<ColumnStatistics>> fileStats;
    private long fileStatsRetainedBytes;

    @Nullable
    private final OrcWriteValidationBuilder validationBuilder;

    public OrcWriter(
            OrcDataSink orcDataSink,
            List<String> columnNames,
            List<Type> types,
            ColumnMetadata<OrcType> orcTypes,
            CompressionKind compression,
            OrcWriterOptions options,
            Map<String, String> userMetadata,
            boolean validate,
            OrcWriteValidationMode validationMode,
            OrcWriterStats stats)
    {
        this.validationBuilder = validate ? new OrcWriteValidationBuilder(validationMode, types)
                .setStringStatisticsLimitInBytes(toIntExact(options.getMaxStringStatisticsLimit().toBytes())) : null;

        this.orcDataSink = requireNonNull(orcDataSink, "orcDataSink is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.compression = requireNonNull(compression, "compression is null");
        recordValidation(validation -> validation.setCompression(compression));
        recordValidation(validation -> validation.setTimeZone(ZoneId.of("UTC")));

        requireNonNull(options, "options is null");
        checkArgument(options.getStripeMaxSize().compareTo(options.getStripeMinSize()) >= 0, "stripeMaxSize must be greater than stripeMinSize");
        int stripeMinBytes = toIntExact(requireNonNull(options.getStripeMinSize(), "stripeMinSize is null").toBytes());
        this.stripeMaxBytes = toIntExact(requireNonNull(options.getStripeMaxSize(), "stripeMaxSize is null").toBytes());
        this.chunkMaxLogicalBytes = Math.max(1, stripeMaxBytes / 2);
        this.stripeMaxRowCount = options.getStripeMaxRowCount();
        this.rowGroupMaxRowCount = options.getRowGroupMaxRowCount();
        recordValidation(validation -> validation.setRowGroupMaxRowCount(rowGroupMaxRowCount));
        this.maxCompressionBufferSize = toIntExact(options.getMaxCompressionBufferSize().toBytes());

        this.userMetadata.putAll(requireNonNull(userMetadata, "userMetadata is null"));
        this.userMetadata.put(TRINO_ORC_WRITER_VERSION_METADATA_KEY, TRINO_ORC_WRITER_VERSION);
        this.metadataWriter = new CompressedMetadataWriter(new OrcMetadataWriter(options.getWriterIdentification()), compression, maxCompressionBufferSize);
        this.stats = requireNonNull(stats, "stats is null");

        requireNonNull(columnNames, "columnNames is null");
        this.orcTypes = requireNonNull(orcTypes, "orcTypes is null");
        recordValidation(validation -> validation.setColumnNames(columnNames));

        // create column writers
        OrcType rootType = orcTypes.get(ROOT_COLUMN);
        checkArgument(rootType.getFieldCount() == types.size());
        ImmutableList.Builder<ColumnWriter> columnWriters = ImmutableList.builder();
        ImmutableSet.Builder<SliceDictionaryColumnWriter> sliceColumnWriters = ImmutableSet.builder();
        for (int fieldId = 0; fieldId < types.size(); fieldId++) {
            OrcColumnId fieldColumnIndex = rootType.getFieldTypeIndex(fieldId);
            Type fieldType = types.get(fieldId);
            ColumnWriter columnWriter = createColumnWriter(
                    fieldColumnIndex,
                    orcTypes,
                    fieldType,
                    compression,
                    maxCompressionBufferSize,
                    options.getMaxStringStatisticsLimit(),
                    getBloomFilterBuilder(options, columnNames.get(fieldId)),
                    options.isShouldCompactMinMax());
            columnWriters.add(columnWriter);

            if (columnWriter instanceof SliceDictionaryColumnWriter) {
                sliceColumnWriters.add((SliceDictionaryColumnWriter) columnWriter);
            }
            else {
                for (ColumnWriter nestedColumnWriter : columnWriter.getNestedColumnWriters()) {
                    if (nestedColumnWriter instanceof SliceDictionaryColumnWriter) {
                        sliceColumnWriters.add((SliceDictionaryColumnWriter) nestedColumnWriter);
                    }
                }
            }
        }
        this.columnWriters = columnWriters.build();
        this.dictionaryCompressionOptimizer = new DictionaryCompressionOptimizer(
                sliceColumnWriters.build(),
                stripeMinBytes,
                stripeMaxBytes,
                stripeMaxRowCount,
                toIntExact(requireNonNull(options.getDictionaryMaxMemory(), "dictionaryMaxMemory is null").toBytes()));

        for (Entry<String, String> entry : this.userMetadata.entrySet()) {
            recordValidation(validation -> validation.addMetadataProperty(entry.getKey(), utf8Slice(entry.getValue())));
        }

        this.previouslyRecordedSizeInBytes = getRetainedBytes();
        stats.updateSizeInBytes(previouslyRecordedSizeInBytes);
    }

    /**
     * Number of bytes already flushed to the data sink.
     */
    public long getWrittenBytes()
    {
        return orcDataSink.size();
    }

    /**
     * Number of pending bytes not yet flushed.
     */
    public int getBufferedBytes()
    {
        return bufferedBytes;
    }

    public int getStripeRowCount()
    {
        return stripeRowCount;
    }

    public long getRetainedBytes()
    {
        return INSTANCE_SIZE +
                columnWritersRetainedBytes +
                closedStripesRetainedBytes +
                orcDataSink.getRetainedSizeInBytes() +
                (validationBuilder == null ? 0 : validationBuilder.getRetainedSize()) +
                fileStatsRetainedBytes;
    }

    public void write(Page page)
            throws IOException
    {
        requireNonNull(page, "page is null");
        if (page.getPositionCount() == 0) {
            return;
        }

        checkArgument(page.getChannelCount() == columnWriters.size());

        if (validationBuilder != null) {
            validationBuilder.addPage(page);
        }

        while (page != null) {
            // align page to row group boundaries
            int chunkRows = min(page.getPositionCount(), min(rowGroupMaxRowCount - rowGroupRowCount, stripeMaxRowCount - stripeRowCount));
            Page chunk = page.getRegion(0, chunkRows);

            // avoid chunk with huge logical size
            while (chunkRows > 1 && chunk.getLogicalSizeInBytes() > chunkMaxLogicalBytes) {
                chunkRows /= 2;
                chunk = chunk.getRegion(0, chunkRows);
            }

            if (chunkRows < page.getPositionCount()) {
                page = page.getRegion(chunkRows, page.getPositionCount() - chunkRows);
            }
            else {
                page = null;
            }

            writeChunk(chunk);
            fileRowCount += chunkRows;
        }

        long recordedSizeInBytes = getRetainedBytes();
        stats.updateSizeInBytes(recordedSizeInBytes - previouslyRecordedSizeInBytes);
        previouslyRecordedSizeInBytes = recordedSizeInBytes;
    }

    private void writeChunk(Page chunk)
            throws IOException
    {
        if (rowGroupRowCount == 0) {
            columnWriters.forEach(ColumnWriter::beginRowGroup);
        }

        // write chunks
        bufferedBytes = 0;
        for (int channel = 0; channel < chunk.getChannelCount(); channel++) {
            ColumnWriter writer = columnWriters.get(channel);
            writer.writeBlock(chunk.getBlock(channel));
            bufferedBytes += writer.getBufferedBytes();
        }

        // update stats
        rowGroupRowCount += chunk.getPositionCount();
        checkState(rowGroupRowCount <= rowGroupMaxRowCount);
        stripeRowCount += chunk.getPositionCount();

        // record checkpoint if necessary
        if (rowGroupRowCount == rowGroupMaxRowCount) {
            finishRowGroup();
        }

        // convert dictionary encoded columns to direct if dictionary memory usage exceeded
        dictionaryCompressionOptimizer.optimize(bufferedBytes, stripeRowCount);

        // flush stripe if necessary
        bufferedBytes = toIntExact(columnWriters.stream().mapToLong(ColumnWriter::getBufferedBytes).sum());
        if (stripeRowCount == stripeMaxRowCount) {
            flushStripe(MAX_ROWS);
        }
        else if (bufferedBytes > stripeMaxBytes) {
            flushStripe(MAX_BYTES);
        }
        else if (dictionaryCompressionOptimizer.isFull(bufferedBytes)) {
            flushStripe(DICTIONARY_FULL);
        }

        columnWritersRetainedBytes = columnWriters.stream().mapToLong(ColumnWriter::getRetainedBytes).sum();
    }

    private void finishRowGroup()
    {
        Map<OrcColumnId, ColumnStatistics> columnStatistics = new HashMap<>();
        columnWriters.forEach(columnWriter -> columnStatistics.putAll(columnWriter.finishRowGroup()));
        recordValidation(validation -> validation.addRowGroupStatistics(columnStatistics));
        rowGroupRowCount = 0;
    }

    private void flushStripe(FlushReason flushReason)
            throws IOException
    {
        List<OrcDataOutput> outputData = new ArrayList<>();
        long stripeStartOffset = orcDataSink.size();
        // add header to first stripe (this is not required but nice to have)
        if (closedStripes.isEmpty()) {
            outputData.add(createDataOutput(MAGIC));
            stripeStartOffset += MAGIC.length();
        }
        // add stripe data
        outputData.addAll(bufferStripeData(stripeStartOffset, flushReason));
        // if the file is being closed, add the file footer
        if (flushReason == CLOSED) {
            outputData.addAll(bufferFileFooter());
        }

        // write all data
        orcDataSink.write(outputData);

        // open next stripe
        columnWriters.forEach(ColumnWriter::reset);
        dictionaryCompressionOptimizer.reset();
        rowGroupRowCount = 0;
        stripeRowCount = 0;
        bufferedBytes = toIntExact(columnWriters.stream().mapToLong(ColumnWriter::getBufferedBytes).sum());
    }

    /**
     * Collect the data for the stripe.  This is not the actual data, but
     * instead are functions that know how to write the data.
     */
    private List<OrcDataOutput> bufferStripeData(long stripeStartOffset, FlushReason flushReason)
            throws IOException
    {
        if (stripeRowCount == 0) {
            verify(flushReason == CLOSED, "An empty stripe is not allowed");
            // column writers must be closed or the reset call will fail
            columnWriters.forEach(ColumnWriter::close);
            return ImmutableList.of();
        }

        if (rowGroupRowCount > 0) {
            finishRowGroup();
        }

        // convert any dictionary encoded column with a low compression ratio to direct
        dictionaryCompressionOptimizer.finalOptimize(bufferedBytes);

        columnWriters.forEach(ColumnWriter::close);

        List<OrcDataOutput> outputData = new ArrayList<>();
        List<Stream> allStreams = new ArrayList<>(columnWriters.size() * 3);

        // get index streams
        long indexLength = 0;
        for (ColumnWriter columnWriter : columnWriters) {
            for (StreamDataOutput indexStream : columnWriter.getIndexStreams(metadataWriter)) {
                // The ordering is critical because the stream only contain a length with no offset.
                outputData.add(indexStream);
                allStreams.add(indexStream.getStream());
                indexLength += indexStream.size();
            }
            for (StreamDataOutput bloomFilter : columnWriter.getBloomFilters(metadataWriter)) {
                outputData.add(bloomFilter);
                allStreams.add(bloomFilter.getStream());
                indexLength += bloomFilter.size();
            }
        }

        // data streams (sorted by size)
        long dataLength = 0;
        List<StreamDataOutput> dataStreams = new ArrayList<>(columnWriters.size() * 2);
        for (ColumnWriter columnWriter : columnWriters) {
            List<StreamDataOutput> streams = columnWriter.getDataStreams();
            dataStreams.addAll(streams);
            dataLength += streams.stream()
                    .mapToLong(StreamDataOutput::size)
                    .sum();
        }
        Collections.sort(dataStreams);

        // add data streams
        for (StreamDataOutput dataStream : dataStreams) {
            // The ordering is critical because the stream only contain a length with no offset.
            outputData.add(dataStream);
            allStreams.add(dataStream.getStream());
        }

        Map<OrcColumnId, ColumnEncoding> columnEncodings = new HashMap<>();
        columnWriters.forEach(columnWriter -> columnEncodings.putAll(columnWriter.getColumnEncodings()));

        Map<OrcColumnId, ColumnStatistics> columnStatistics = new HashMap<>();
        columnWriters.forEach(columnWriter -> columnStatistics.putAll(columnWriter.getColumnStripeStatistics()));

        // the 0th column is a struct column for the whole row
        columnEncodings.put(ROOT_COLUMN, new ColumnEncoding(DIRECT, 0));
        columnStatistics.put(ROOT_COLUMN, new ColumnStatistics((long) stripeRowCount, 0, null, null, null, null, null, null, null, null, null, null));

        // add footer
        StripeFooter stripeFooter = new StripeFooter(allStreams, toColumnMetadata(columnEncodings, orcTypes.size()), ZoneId.of("UTC"));
        Slice footer = metadataWriter.writeStripeFooter(stripeFooter);
        outputData.add(createDataOutput(footer));

        // create final stripe statistics
        StripeStatistics statistics = new StripeStatistics(toColumnMetadata(columnStatistics, orcTypes.size()));
        recordValidation(validation -> validation.addStripeStatistics(stripeStartOffset, statistics));
        StripeInformation stripeInformation = new StripeInformation(stripeRowCount, stripeStartOffset, indexLength, dataLength, footer.length());
        ClosedStripe closedStripe = new ClosedStripe(stripeInformation, statistics);
        closedStripes.add(closedStripe);
        closedStripesRetainedBytes += closedStripe.getRetainedSizeInBytes();
        recordValidation(validation -> validation.addStripe(stripeInformation.getNumberOfRows()));
        stats.recordStripeWritten(flushReason, stripeInformation.getTotalLength(), stripeInformation.getNumberOfRows(), dictionaryCompressionOptimizer.getDictionaryMemoryBytes());

        return outputData;
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        stats.updateSizeInBytes(-previouslyRecordedSizeInBytes);
        previouslyRecordedSizeInBytes = 0;

        try (Closeable ignored = orcDataSink) {
            flushStripe(CLOSED);
        }

        bufferedBytes = 0;
    }

    public enum OrcOperation
    {
        NONE(-1),
        INSERT(0),
        DELETE(2);

        private final int operationNumber;

        OrcOperation(int operationNumber)
        {
            this.operationNumber = operationNumber;
        }

        public int getOperationNumber()
        {
            return operationNumber;
        }
    }

    public void updateUserMetadata(Map<String, String> updatedProperties)
    {
        userMetadata.putAll(updatedProperties);
    }

    /**
     * Collect the data for the file footer.  This is not the actual data, but
     * instead are functions that know how to write the data.
     */
    private List<OrcDataOutput> bufferFileFooter()
            throws IOException
    {
        List<OrcDataOutput> outputData = new ArrayList<>();

        Metadata metadata = new Metadata(closedStripes.stream()
                .map(ClosedStripe::getStatistics)
                .map(Optional::of)
                .collect(toList()));
        Slice metadataSlice = metadataWriter.writeMetadata(metadata);
        outputData.add(createDataOutput(metadataSlice));

        fileStats = toFileStats(closedStripes.stream()
                .map(ClosedStripe::getStatistics)
                .map(StripeStatistics::getColumnStatistics)
                .collect(toList()));
        fileStatsRetainedBytes = fileStats.map(stats -> stats.stream()
                .mapToLong(ColumnStatistics::getRetainedSizeInBytes)
                .sum()).orElse(0L);
        recordValidation(validation -> validation.setFileStatistics(fileStats));

        Map<String, Slice> userMetadata = this.userMetadata.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> utf8Slice(entry.getValue())));

        Footer footer = new Footer(
                fileRowCount,
                rowGroupMaxRowCount == 0 ? OptionalInt.empty() : OptionalInt.of(rowGroupMaxRowCount),
                closedStripes.stream()
                        .map(ClosedStripe::getStripeInformation)
                        .collect(toImmutableList()),
                orcTypes,
                fileStats,
                userMetadata,
                Optional.empty()); // writer id will be set by MetadataWriter

        closedStripes.clear();
        closedStripesRetainedBytes = 0;

        Slice footerSlice = metadataWriter.writeFooter(footer);
        outputData.add(createDataOutput(footerSlice));

        recordValidation(validation -> validation.setVersion(metadataWriter.getOrcMetadataVersion()));
        Slice postscriptSlice = metadataWriter.writePostscript(footerSlice.length(), metadataSlice.length(), compression, maxCompressionBufferSize);
        outputData.add(createDataOutput(postscriptSlice));
        outputData.add(createDataOutput(Slices.wrappedBuffer(UnsignedBytes.checkedCast(postscriptSlice.length()))));
        return outputData;
    }

    private void recordValidation(Consumer<OrcWriteValidationBuilder> task)
    {
        if (validationBuilder != null) {
            task.accept(validationBuilder);
        }
    }

    public void validate(OrcDataSource input)
            throws OrcCorruptionException
    {
        checkState(validationBuilder != null, "validation is not enabled");
        validateFile(validationBuilder.build(), input, types);
    }

    public long getFileRowCount()
    {
        checkState(closed, "File row count is not available until the writing has finished");
        return fileRowCount;
    }

    public Optional<ColumnMetadata<ColumnStatistics>> getFileStats()
    {
        checkState(closed, "File statistics are not available until the writing has finished");
        return fileStats;
    }

    private static Supplier<BloomFilterBuilder> getBloomFilterBuilder(OrcWriterOptions options, String columnName)
    {
        if (options.isBloomFilterColumn(columnName)) {
            return () -> new Utf8BloomFilterBuilder(options.getRowGroupMaxRowCount(), options.getBloomFilterFpp());
        }
        return NoOpBloomFilterBuilder::new;
    }

    private static <T> ColumnMetadata<T> toColumnMetadata(Map<OrcColumnId, T> data, int expectedSize)
    {
        checkArgument(data.size() == expectedSize);
        List<T> list = new ArrayList<>(expectedSize);
        for (int i = 0; i < expectedSize; i++) {
            list.add(data.get(new OrcColumnId(i)));
        }
        return new ColumnMetadata<>(ImmutableList.copyOf(list));
    }

    private static Optional<ColumnMetadata<ColumnStatistics>> toFileStats(List<ColumnMetadata<ColumnStatistics>> stripes)
    {
        if (stripes.isEmpty()) {
            return Optional.empty();
        }
        int columnCount = stripes.get(0).size();
        checkArgument(stripes.stream().allMatch(stripe -> columnCount == stripe.size()));

        ImmutableList.Builder<ColumnStatistics> fileStats = ImmutableList.builder();
        for (int i = 0; i < columnCount; i++) {
            OrcColumnId columnId = new OrcColumnId(i);
            fileStats.add(ColumnStatistics.mergeColumnStatistics(stripes.stream()
                    .map(stripe -> stripe.get(columnId))
                    .collect(toList())));
        }
        return Optional.of(new ColumnMetadata<>(fileStats.build()));
    }

    private static class ClosedStripe
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(ClosedStripe.class).instanceSize() + ClassLayout.parseClass(StripeInformation.class).instanceSize();

        private final StripeInformation stripeInformation;
        private final StripeStatistics statistics;

        public ClosedStripe(StripeInformation stripeInformation, StripeStatistics statistics)
        {
            this.stripeInformation = requireNonNull(stripeInformation, "stripeInformation is null");
            this.statistics = requireNonNull(statistics, "statistics is null");
        }

        public StripeInformation getStripeInformation()
        {
            return stripeInformation;
        }

        public StripeStatistics getStatistics()
        {
            return statistics;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + statistics.getRetainedSizeInBytes();
        }
    }
}
