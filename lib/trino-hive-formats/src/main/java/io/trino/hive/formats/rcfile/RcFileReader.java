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
package io.trino.hive.formats.rcfile;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.FormatMethod;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.FileCorruptionException;
import io.trino.hive.formats.ReadWriteUtils;
import io.trino.hive.formats.TrinoDataInputStream;
import io.trino.hive.formats.compression.Codec;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.compression.ValueDecompressor;
import io.trino.hive.formats.encodings.ColumnData;
import io.trino.hive.formats.encodings.ColumnEncoding;
import io.trino.hive.formats.encodings.ColumnEncodingFactory;
import io.trino.hive.formats.rcfile.RcFileWriteValidation.WriteChecksum;
import io.trino.hive.formats.rcfile.RcFileWriteValidation.WriteChecksumBuilder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.ByteStreams.skipFully;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.hive.formats.compression.CompressionKind.LZOP;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class RcFileReader
        implements Closeable
{
    private static final Slice RCFILE_MAGIC = Slices.utf8Slice("RCF");

    // These numbers were chosen arbitrarily, and are only to prevent corrupt files from causing OOMs
    private static final int MAX_METADATA_ENTRIES = 500_000;
    private static final int MAX_COLUMN_COUNT = 500_000;
    private static final int MAX_METADATA_STRING_LENGTH = 1024 * 1024;

    private static final int FIRST_VERSION = 0;
    private static final int CURRENT_VERSION = 1;

    // First version of RCFile use Sequence file magic with version 6
    private static final Slice SEQUENCE_FILE_MAGIC = Slices.utf8Slice("SEQ");
    private static final byte SEQUENCE_FILE_VERSION = 6;
    private static final Slice RCFILE_KEY_BUFFER_NAME = Slices.utf8Slice("org.apache.hadoop.hive.ql.io.RCFile$KeyBuffer");
    private static final Slice RCFILE_VALUE_BUFFER_NAME = Slices.utf8Slice("org.apache.hadoop.hive.ql.io.RCFile$ValueBuffer");

    private static final String COLUMN_COUNT_METADATA_KEY = "hive.io.rcfile.column.number";

    private final Location location;
    private final long fileSize;
    private final Map<Integer, Type> readColumns;
    private final TrinoDataInputStream input;
    private final long length;

    private final byte version;

    private final ValueDecompressor decompressor;

    private final Map<String, String> metadata;
    private final int columnCount;

    private final long syncFirst;
    private final long syncSecond;

    private final Column[] columns;
    private final long end;

    private long rowsRead;

    private int rowGroupRowCount;
    private int rowGroupPosition;

    private int currentChunkRowCount;

    private Slice compressedHeaderBuffer = Slices.EMPTY_SLICE;
    private Slice headerBuffer = Slices.EMPTY_SLICE;

    private boolean closed;

    private final Optional<RcFileWriteValidation> writeValidation;
    private final Optional<WriteChecksumBuilder> writeChecksumBuilder;

    public RcFileReader(
            TrinoInputFile inputFile,
            ColumnEncodingFactory encoding,
            Map<Integer, Type> readColumns,
            long offset,
            long length)
            throws IOException
    {
        this(inputFile, encoding, readColumns, offset, length, Optional.empty());
    }

    private RcFileReader(
            TrinoInputFile inputFile,
            ColumnEncodingFactory encoding,
            Map<Integer, Type> readColumns,
            long offset,
            long length,
            Optional<RcFileWriteValidation> writeValidation)
            throws IOException
    {
        requireNonNull(inputFile, "inputFile is null");
        this.location = inputFile.location();
        this.fileSize = inputFile.length();
        this.readColumns = ImmutableMap.copyOf(requireNonNull(readColumns, "readColumns is null"));

        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");
        this.writeChecksumBuilder = writeValidation.map(validation -> WriteChecksumBuilder.createWriteChecksumBuilder(readColumns));

        verify(offset >= 0, "offset is negative");
        verify(offset < fileSize, "offset is greater than data size");
        verify(length >= 1, "length must be at least 1");
        this.length = length;
        this.end = offset + length;
        verify(end <= fileSize, "offset plus length is greater than data size");

        this.input = new TrinoDataInputStream(inputFile.newStream());
        try {
            // read header
            Slice magic = input.readSlice(RCFILE_MAGIC.length());
            boolean compressed;
            if (RCFILE_MAGIC.equals(magic)) {
                version = input.readByte();
                verify(version <= CURRENT_VERSION, "RCFile version %s not supported: %s", version, inputFile.location());
                validateWrite(validation -> validation.getVersion() == version, "Unexpected file version");
                compressed = input.readBoolean();
            }
            else if (SEQUENCE_FILE_MAGIC.equals(magic)) {
                validateWrite(validation -> false, "Expected file to start with RCFile magic");

                // first version of RCFile used magic SEQ with version 6
                byte sequenceFileVersion = input.readByte();
                verify(sequenceFileVersion == SEQUENCE_FILE_VERSION, "File %s is a SequenceFile not an RCFile", inputFile.location());

                // this is the first version of RCFile
                this.version = FIRST_VERSION;

                Slice keyClassName = readLengthPrefixedString(input);
                Slice valueClassName = readLengthPrefixedString(input);
                verify(RCFILE_KEY_BUFFER_NAME.equals(keyClassName) && RCFILE_VALUE_BUFFER_NAME.equals(valueClassName), "File %s is a SequenceFile not an RCFile", inputFile);
                compressed = input.readBoolean();

                // RC file is never block compressed
                if (input.readBoolean()) {
                    throw corrupt("File %s is a SequenceFile not an RCFile", inputFile.location());
                }
            }
            else {
                throw corrupt("File %s is not an RCFile", inputFile.location());
            }

            // setup the compression codec
            if (compressed) {
                String codecClassName = readLengthPrefixedString(input).toStringUtf8();
                CompressionKind compressionKind = CompressionKind.fromHadoopClassName(codecClassName);
                checkArgument(compressionKind != LZOP, "LZOP cannot be use with RCFile.  LZO compression can be used, but LZ4 is preferred.");
                Codec codecFromHadoopClassName = compressionKind.createCodec();
                validateWrite(validation -> validation.getCodecClassName().equals(Optional.of(codecClassName)), "Unexpected compression codec");
                this.decompressor = codecFromHadoopClassName.createValueDecompressor();
            }
            else {
                validateWrite(validation -> validation.getCodecClassName().equals(Optional.empty()), "Expected file to be compressed");
                this.decompressor = null;
            }

            // read metadata
            int metadataEntries = Integer.reverseBytes(input.readInt());
            verify(metadataEntries >= 0, "Invalid metadata entry count %s in RCFile %s", metadataEntries, inputFile.location());
            verify(metadataEntries <= MAX_METADATA_ENTRIES, "Too many metadata entries (%s) in RCFile %s", metadataEntries, inputFile.location());
            ImmutableMap.Builder<String, String> metadataBuilder = ImmutableMap.builder();
            for (int i = 0; i < metadataEntries; i++) {
                metadataBuilder.put(readLengthPrefixedString(input).toStringUtf8(), readLengthPrefixedString(input).toStringUtf8());
            }
            metadata = metadataBuilder.buildOrThrow();
            validateWrite(validation -> validation.getMetadata().equals(metadata), "Unexpected metadata");

            // get column count from metadata
            String columnCountString = metadata.get(COLUMN_COUNT_METADATA_KEY);
            verify(columnCountString != null, "Column count not specified in metadata RCFile %s", inputFile.location());
            try {
                columnCount = Integer.parseInt(columnCountString);
            }
            catch (NumberFormatException e) {
                throw corrupt("Invalid column count %s in RCFile %s", columnCountString, inputFile.location());
            }

            // initialize columns
            verify(columnCount <= MAX_COLUMN_COUNT, "Too many columns (%s) in RCFile %s", columnCountString, inputFile.location());
            columns = new Column[columnCount];
            for (Entry<Integer, Type> entry : readColumns.entrySet()) {
                if (entry.getKey() < columnCount) {
                    ColumnEncoding columnEncoding = encoding.getEncoding(entry.getValue());
                    columns[entry.getKey()] = new Column(columnEncoding, decompressor);
                }
            }

            // read sync bytes
            syncFirst = input.readLong();
            validateWrite(validation -> validation.getSyncFirst() == syncFirst, "Unexpected sync sequence");
            syncSecond = input.readLong();
            validateWrite(validation -> validation.getSyncSecond() == syncSecond, "Unexpected sync sequence");

            // seek to first sync point within the specified region, unless the region starts at the beginning
            // of the file.  In that case, the reader owns all row groups up to the first sync point.
            if (offset != 0) {
                // if the specified file region does not contain the start of a sync sequence, this call will close the reader
                long startOfSyncSequence = ReadWriteUtils.findFirstSyncPosition(inputFile, offset, length, syncFirst, syncSecond);
                if (startOfSyncSequence < 0) {
                    closeQuietly();
                    return;
                }
                input.seek(startOfSyncSequence);
            }
        }
        catch (Throwable throwable) {
            try (input) {
                throw throwable;
            }
        }
    }

    public byte getVersion()
    {
        return version;
    }

    public Map<String, String> getMetadata()
    {
        return metadata;
    }

    public int getColumnCount()
    {
        return columnCount;
    }

    public long getLength()
    {
        return length;
    }

    public long getBytesRead()
    {
        return input.getReadBytes();
    }

    public long getRowsRead()
    {
        return rowsRead;
    }

    public long getReadTimeNanos()
    {
        return input.getReadTimeNanos();
    }

    public Slice getSync()
    {
        Slice sync = Slices.allocate(SIZE_OF_LONG + SIZE_OF_LONG);
        sync.setLong(0, syncFirst);
        sync.setLong(SIZE_OF_LONG, syncSecond);
        return sync;
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        rowGroupPosition = 0;
        rowGroupRowCount = 0;
        currentChunkRowCount = 0;
        input.close();
        if (writeChecksumBuilder.isPresent()) {
            WriteChecksum actualChecksum = writeChecksumBuilder.get().build();
            validateWrite(validation -> validation.getChecksum().getTotalRowCount() == actualChecksum.getTotalRowCount(), "Invalid row count");
            List<Long> columnHashes = actualChecksum.getColumnHashes();
            for (int i = 0; i < columnHashes.size(); i++) {
                int columnIndex = i;
                validateWrite(validation -> validation.getChecksum().getColumnHashes().get(columnIndex).equals(columnHashes.get(columnIndex)),
                        "Invalid checksum for column %s", columnIndex);
            }
            validateWrite(validation -> validation.getChecksum().getRowGroupHash() == actualChecksum.getRowGroupHash(), "Invalid row group checksum");
        }
    }

    public int advance()
            throws IOException
    {
        if (closed) {
            return -1;
        }
        rowGroupPosition += ColumnData.MAX_SIZE;
        currentChunkRowCount = min(ColumnData.MAX_SIZE, rowGroupRowCount - rowGroupPosition);

        // do we still have rows in the current row group
        if (currentChunkRowCount > 0) {
            validateWritePageChecksum();
            return currentChunkRowCount;
        }

        // are we at the end?
        if (fileSize - input.getPos() == 0) {
            close();
            return -1;
        }

        // read uncompressed size of row group (which is useless information)
        verify(fileSize - input.getPos() >= SIZE_OF_INT, "RCFile truncated %s", location);
        int unusedRowGroupSize = Integer.reverseBytes(input.readInt());

        // read sequence sync if present
        if (unusedRowGroupSize == -1) {
            verify(fileSize - input.getPos() >= SIZE_OF_LONG + SIZE_OF_LONG + SIZE_OF_INT, "RCFile truncated %s", length);

            // The full sync sequence is "0xFFFFFFFF syncFirst syncSecond".  If
            // this sequence begins in our segment, we must continue process until the
            // next sync sequence.
            // We have already read the 0xFFFFFFFF above, so we must test the
            // end condition back 4 bytes.
            // NOTE: this decision must agree with RcFileDecoderUtils.findFirstSyncPosition
            if (input.getPos() - SIZE_OF_INT >= end) {
                close();
                return -1;
            }

            verify(syncFirst == input.readLong() && syncSecond == input.readLong(), "Invalid sync in RCFile %s", location);

            // read the useless uncompressed length
            unusedRowGroupSize = Integer.reverseBytes(input.readInt());
        }
        else if (rowsRead > 0) {
            validateWrite(writeValidation -> false, "Expected sync sequence for every row group except the first one");
        }
        verify(unusedRowGroupSize > 0, "Invalid uncompressed row group length %s", unusedRowGroupSize);

        // read row group header
        int uncompressedHeaderSize = Integer.reverseBytes(input.readInt());
        int compressedHeaderSize = Integer.reverseBytes(input.readInt());
        if (compressedHeaderSize > compressedHeaderBuffer.length()) {
            compressedHeaderBuffer = Slices.allocate(compressedHeaderSize);
        }
        // use exact sized compressed header to avoid problems where compression algorithms over read
        Slice compressedHeader = compressedHeaderBuffer.slice(0, compressedHeaderSize);
        input.readFully(compressedHeader);

        // decompress row group header
        Slice header;
        if (decompressor != null) {
            if (headerBuffer.length() < uncompressedHeaderSize) {
                headerBuffer = Slices.allocate(uncompressedHeaderSize);
            }
            Slice buffer = headerBuffer.slice(0, uncompressedHeaderSize);

            decompressor.decompress(compressedHeader, buffer);

            header = buffer;
        }
        else {
            verify(compressedHeaderSize == uncompressedHeaderSize, "Invalid RCFile %s", location);
            header = compressedHeader;
        }
        BasicSliceInput headerInput = header.getInput();

        // read number of rows in row group
        rowGroupRowCount = toIntExact(ReadWriteUtils.readVInt(headerInput));
        rowsRead += rowGroupRowCount;
        rowGroupPosition = 0;
        currentChunkRowCount = min(ColumnData.MAX_SIZE, rowGroupRowCount);

        // set column buffers
        int totalCompressedDataSize = 0;
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int compressedDataSize = toIntExact(ReadWriteUtils.readVInt(headerInput));
            totalCompressedDataSize += compressedDataSize;
            int uncompressedDataSize = toIntExact(ReadWriteUtils.readVInt(headerInput));
            if (decompressor == null && compressedDataSize != uncompressedDataSize) {
                throw corrupt("Invalid RCFile %s", location);
            }

            int lengthsSize = toIntExact(ReadWriteUtils.readVInt(headerInput));

            Slice lengthsBuffer = headerInput.readSlice(lengthsSize);

            if (readColumns.containsKey(columnIndex)) {
                Slice dataBuffer = input.readSlice(compressedDataSize);
                columns[columnIndex].setBuffers(lengthsBuffer, dataBuffer, uncompressedDataSize);
            }
            else {
                skipFully(input, compressedDataSize);
            }
        }

        // this value is not used but validate it is correct since it might signal corruption
        verify(unusedRowGroupSize == totalCompressedDataSize + uncompressedHeaderSize, "Invalid row group size");

        validateWriteRowGroupChecksum();
        validateWritePageChecksum();
        return currentChunkRowCount;
    }

    public Block readBlock(int columnIndex)
            throws IOException
    {
        checkArgument(readColumns.containsKey(columnIndex), "Column '%s' is not being read", columnIndex);
        checkState(currentChunkRowCount > 0, "No more data");

        if (columnIndex >= columns.length) {
            return RunLengthEncodedBlock.create(readColumns.get(columnIndex).createNullBlock(), currentChunkRowCount);
        }

        return columns[columnIndex].readBlock(rowGroupPosition, currentChunkRowCount);
    }

    public Location getFileLocation()
    {
        return location;
    }

    private void closeQuietly()
    {
        try {
            close();
        }
        catch (IOException _) {
        }
    }

    private Slice readLengthPrefixedString(TrinoDataInputStream in)
            throws IOException
    {
        int length = toIntExact(ReadWriteUtils.readVInt(in));
        verify(length <= MAX_METADATA_STRING_LENGTH, "Metadata string value is too long (%s) in RCFile %s", length, location);
        return in.readSlice(length);
    }

    @FormatMethod
    private void verify(boolean expression, String messageFormat, Object... args)
            throws FileCorruptionException
    {
        if (!expression) {
            throw corrupt(messageFormat, args);
        }
    }

    @FormatMethod
    private FileCorruptionException corrupt(String messageFormat, Object... args)
    {
        closeQuietly();
        return new FileCorruptionException(messageFormat, args);
    }

    @SuppressWarnings("FormatStringAnnotation")
    @FormatMethod
    private void validateWrite(Predicate<RcFileWriteValidation> test, String messageFormat, Object... args)
            throws FileCorruptionException
    {
        if (writeValidation.isPresent() && !test.test(writeValidation.get())) {
            throw corrupt("Write validation failed: " + messageFormat, args);
        }
    }

    private void validateWriteRowGroupChecksum()
    {
        writeChecksumBuilder.ifPresent(checksumBuilder -> checksumBuilder.addRowGroup(rowGroupRowCount));
    }

    private void validateWritePageChecksum()
            throws IOException
    {
        if (writeChecksumBuilder.isPresent()) {
            Block[] blocks = new Block[columns.length];
            for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
                blocks[columnIndex] = readBlock(columnIndex);
            }
            writeChecksumBuilder.get().addPage(new Page(currentChunkRowCount, blocks));
        }
    }

    static void validateFile(
            RcFileWriteValidation writeValidation,
            TrinoInputFile inputFile,
            ColumnEncodingFactory encoding,
            List<Type> types)
            throws FileCorruptionException
    {
        ImmutableMap.Builder<Integer, Type> readTypes = ImmutableMap.builder();
        for (int columnIndex = 0; columnIndex < types.size(); columnIndex++) {
            readTypes.put(columnIndex, types.get(columnIndex));
        }
        try (RcFileReader rcFileReader = new RcFileReader(
                inputFile,
                encoding,
                readTypes.buildOrThrow(),
                0,
                inputFile.length(),
                Optional.of(writeValidation))) {
            while (rcFileReader.advance() >= 0) {
                // ignored
            }
        }
        catch (FileCorruptionException e) {
            throw e;
        }
        catch (IOException e) {
            throw new FileCorruptionException(e, "Validation failed");
        }
    }

    private static class Column
    {
        private final ColumnEncoding encoding;
        private final ValueDecompressor decompressor;

        private BasicSliceInput lengthsInput;
        private Slice dataBuffer;
        private int uncompressedDataSize;

        private byte[] decompressedBuffer = new byte[0];

        private boolean compressed;

        private int currentPosition;

        private int currentOffset;
        private int runLength;
        private int lastValueLength = -1;

        public Column(ColumnEncoding encoding, ValueDecompressor decompressor)
        {
            this.encoding = encoding;
            this.decompressor = decompressor;
        }

        public void setBuffers(Slice lengthsBuffer, Slice dataBuffer, int uncompressedDataSize)
        {
            this.lengthsInput = lengthsBuffer.getInput();
            this.dataBuffer = dataBuffer;
            this.uncompressedDataSize = uncompressedDataSize;

            compressed = (decompressor != null);

            currentPosition = 0;
            currentOffset = 0;
            runLength = 0;
            lastValueLength = 0;
        }

        public Block readBlock(int position, int size)
                throws IOException
        {
            checkArgument(size > 0 && size <= ColumnData.MAX_SIZE, "Invalid size");
            checkArgument(currentPosition <= position, "Invalid position");

            if (currentPosition < position) {
                skipTo(position);
            }

            // read offsets
            int[] offsets = readOffsets(size);

            ColumnData columnData = new ColumnData(offsets, getDataBuffer());
            Block block = encoding.decodeColumn(columnData);
            return block;
        }

        private int[] readOffsets(int batchSize)
                throws IOException
        {
            int[] offsets = new int[batchSize + 1];
            offsets[0] = currentOffset;
            for (int i = 0; i < batchSize; i++) {
                int valueLength = readNextValueLength();
                offsets[i + 1] = offsets[i] + valueLength;
            }
            currentOffset = offsets[batchSize];
            currentPosition += batchSize;
            return offsets;
        }

        private void skipTo(int position)
                throws IOException
        {
            checkArgument(currentPosition <= position, "Invalid position");
            for (; currentPosition < position; currentPosition++) {
                int valueLength = readNextValueLength();
                currentOffset += valueLength;
            }
        }

        private int readNextValueLength()
                throws IOException
        {
            if (runLength > 0) {
                runLength--;
                return lastValueLength;
            }

            int valueLength = toIntExact(ReadWriteUtils.readVInt(lengthsInput));

            // negative length is used to encode a run or the last value
            if (valueLength < 0) {
                if (lastValueLength == -1) {
                    throw new FileCorruptionException("First column value length is negative");
                }
                runLength = ~valueLength - 1;
                return lastValueLength;
            }

            runLength = 0;
            lastValueLength = valueLength;
            return valueLength;
        }

        private Slice getDataBuffer()
                throws IOException
        {
            if (compressed) {
                if (decompressedBuffer.length < uncompressedDataSize) {
                    decompressedBuffer = new byte[uncompressedDataSize];
                }
                Slice buffer = Slices.wrappedBuffer(decompressedBuffer, 0, uncompressedDataSize);

                decompressor.decompress(dataBuffer, buffer);

                dataBuffer = buffer;
                compressed = false;
            }
            return dataBuffer;
        }
    }
}
