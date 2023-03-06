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
package io.trino.hive.formats.line.sequence;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.errorprone.annotations.FormatMethod;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.FileCorruptionException;
import io.trino.hive.formats.TrinoDataInputStream;
import io.trino.hive.formats.compression.Codec;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.compression.ValueDecompressor;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.hive.formats.ReadWriteUtils.findFirstSyncPosition;
import static io.trino.hive.formats.ReadWriteUtils.readVInt;
import static io.trino.hive.formats.compression.CompressionKind.LZOP;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Sequence file reader hard coded the behavior of Hive Sequence file with a Text value class.
 */
public final class SequenceFileReader
        implements LineReader
{
    private static final int INSTANCE_SIZE = instanceSize(SequenceFileReader.class);

    private static final Slice SEQUENCE_FILE_MAGIC = utf8Slice("SEQ");
    private static final byte SEQUENCE_FILE_VERSION = 6;

    // These numbers were chosen arbitrarily, and are only to prevent corrupt files from causing OOMs
    private static final int MAX_METADATA_ENTRIES = 500_000;
    private static final int MAX_METADATA_STRING_LENGTH = 1024 * 1024;

    private final String location;
    private final TrinoDataInputStream input;

    private final String keyClassName;
    private final String valueClassName;

    private final Map<String, String> metadata;

    private final long syncFirst;
    private final long syncSecond;

    private long rowsRead;

    private final ValueReader valueReader;

    private boolean closed;
    private final Closer closer = Closer.create();

    public SequenceFileReader(TrinoInputFile inputFile, long offset, long length)
            throws IOException
    {
        try {
            requireNonNull(inputFile, "inputFile is null");
            this.location = inputFile.location();
            this.input = new TrinoDataInputStream(inputFile.newStream());
            closer.register(input);

            verify(offset >= 0, "offset is negative");
            verify(offset < inputFile.length(), "offset is greater than data size");
            verify(length >= 1, "length must be at least 1");

            long end = offset + length;
            long fileSize = inputFile.length();
            verify(end <= fileSize, "offset plus length is greater than data size");

            // read header
            Slice magic = input.readSlice(SEQUENCE_FILE_MAGIC.length());
            boolean compressed;
            verify(SEQUENCE_FILE_MAGIC.equals(magic), "File %s is not a Sequence File", location);

            // Only version 6 is supported
            byte version = input.readByte();
            // It would not be too difficult to support older versions, but there is no way to generate test files
            verify(version == SEQUENCE_FILE_VERSION, "Sequence File %s version %s is not supported", location, version);

            keyClassName = readLengthPrefixedString(input).toStringUtf8();
            valueClassName = readLengthPrefixedString(input).toStringUtf8();
            verify("org.apache.hadoop.io.Text".equals(valueClassName), "Sequence File %s value class %s is not supported", location, valueClassName);
            compressed = input.readBoolean();

            // block compression not supported yet
            boolean blockCompression = input.readBoolean();
            verify(!blockCompression || compressed, "Uncompressed Sequence File %s has block compression enabled", location);

            // create the compression codec
            ValueDecompressor decompressor;
            if (compressed) {
                String codecClassName = readLengthPrefixedString(input).toStringUtf8();
                CompressionKind compressionKind = CompressionKind.fromHadoopClassName(codecClassName);
                checkArgument(compressionKind != LZOP, "LZOP cannot be used with SequenceFile. LZO compression can be used, but LZ4 is preferred.");
                Codec codecFromHadoopClassName = compressionKind.createCodec();
                decompressor = codecFromHadoopClassName.createValueDecompressor();
            }
            else {
                decompressor = null;
            }

            // read metadata
            int metadataEntries = Integer.reverseBytes(input.readInt());
            verify(metadataEntries >= 0, "Invalid metadata entry count %s in Sequence File %s", metadataEntries, location);
            verify(metadataEntries <= MAX_METADATA_ENTRIES, "Too many metadata entries (%s) in Sequence File %s", metadataEntries, location);
            ImmutableMap.Builder<String, String> metadataBuilder = ImmutableMap.builder();
            for (int i = 0; i < metadataEntries; i++) {
                String key = readLengthPrefixedString(input).toStringUtf8();
                String value = readLengthPrefixedString(input).toStringUtf8();
                metadataBuilder.put(key, value);
            }
            metadata = metadataBuilder.buildOrThrow();

            // read sync bytes
            syncFirst = input.readLong();
            syncSecond = input.readLong();

            // seek to first sync point within the specified region, unless the region starts at the beginning
            // of the file.  In that case, the reader owns all row groups up to the first sync point.
            if (offset != 0) {
                // if the specified file region does not contain the start of a sync sequence, this call will close the reader
                long startOfSyncSequence = findFirstSyncPosition(inputFile, offset, length, syncFirst, syncSecond);
                if (startOfSyncSequence < 0) {
                    close();
                    valueReader = null;
                    return;
                }
                input.seek(startOfSyncSequence);
            }

            if (blockCompression) {
                valueReader = new BlockCompressedValueReader(location, fileSize, input, decompressor, end, syncFirst, syncSecond);
            }
            else {
                valueReader = new SingleValueReader(location, fileSize, input, decompressor, end, syncFirst, syncSecond);
            }
        }
        catch (Throwable e) {
            try (closer) {
                throw e;
            }
        }
    }

    public String getFileLocation()
    {
        return location;
    }

    public String getKeyClassName()
    {
        return keyClassName;
    }

    public String getValueClassName()
    {
        return valueClassName;
    }

    public Map<String, String> getMetadata()
    {
        return metadata;
    }

    @Override
    public long getBytesRead()
    {
        return input.getReadBytes();
    }

    public long getRowsRead()
    {
        return rowsRead;
    }

    @Override
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
    public long getRetainedSize()
    {
        return INSTANCE_SIZE + input.getRetainedSize() + valueReader.getRetainedSize();
    }

    @Override
    public boolean isClosed()
    {
        return closed;
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        closer.close();
    }

    @Override
    public boolean readLine(LineBuffer lineBuffer)
            throws IOException
    {
        lineBuffer.reset();

        if (closed) {
            return false;
        }

        boolean readLine;
        try {
            readLine = valueReader.readLine(lineBuffer);
        }
        catch (Throwable e) {
            try (Closeable ignored = this) {
                throw e;
            }
        }

        if (readLine) {
            rowsRead++;
        }
        else {
            close();
        }
        return readLine;
    }

    private interface ValueReader
    {
        long getRetainedSize();

        boolean readLine(LineBuffer lineBuffer)
                throws IOException;
    }

    private static class SingleValueReader
            implements ValueReader
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleValueReader.class);

        private final String location;
        private final long fileSize;
        private final TrinoDataInputStream input;
        private final ValueDecompressor decompressor;
        private final long end;

        private final long syncFirst;
        private final long syncSecond;

        private final DynamicSliceOutput compressedBuffer = new DynamicSliceOutput(0);
        private final DynamicSliceOutput uncompressedBuffer = new DynamicSliceOutput(0);

        public SingleValueReader(
                String location,
                long fileSize,
                TrinoDataInputStream input,
                ValueDecompressor decompressor,
                long end,
                long syncFirst,
                long syncSecond)
        {
            this.location = requireNonNull(location, "location is null");
            this.fileSize = fileSize;
            this.input = requireNonNull(input, "input is null");
            this.decompressor = decompressor;
            this.end = end;
            this.syncFirst = syncFirst;
            this.syncSecond = syncSecond;
        }

        @Override
        public long getRetainedSize()
        {
            return INSTANCE_SIZE + input.getRetainedSize() + compressedBuffer.getRetainedSize() + uncompressedBuffer.getRetainedSize();
        }

        @Override
        public boolean readLine(LineBuffer lineBuffer)
                throws IOException
        {
            // are we at the end?
            if (fileSize - input.getPos() == 0) {
                return false;
            }

            // read uncompressed size of row group (which is useless information)
            verify(fileSize - input.getPos() >= SIZE_OF_INT, "SequenceFile truncated %s", location);
            int recordLength = Integer.reverseBytes(input.readInt());

            // read sequence sync if present
            if (recordLength == -1) {
                verify(fileSize - input.getPos() >= SIZE_OF_LONG + SIZE_OF_LONG, "SequenceFile truncated %s", location);

                // The full sync sequence is "0xFFFFFFFF syncFirst syncSecond".  If
                // this sequence begins in our segment, we must continue process until the
                // next sync sequence.
                // We have already read the 0xFFFFFFFF above, so we must test the
                // end condition back 4 bytes.
                // NOTE: this decision must agree with ReadWriteUtils.findFirstSyncPosition
                if (input.getPos() - SIZE_OF_INT >= end) {
                    return false;
                }

                verify(syncFirst == input.readLong() && syncSecond == input.readLong(), "Invalid sync in SequenceFile %s", location);

                // file can end after the sequence marker
                if (fileSize - input.getPos() == 0) {
                    return false;
                }

                // read the useless uncompressed length
                verify(fileSize - input.getPos() >= SIZE_OF_INT, "SequenceFile truncated %s", location);
                recordLength = Integer.reverseBytes(input.readInt());
            }
            verify(recordLength > 0, "Invalid block record count %s", recordLength);

            int keyLength = Integer.reverseBytes(input.readInt());
            verify(keyLength >= 0, "Invalid SequenceFile %s: key length size is negative", location);
            input.skipNBytes(keyLength);

            int compressedValueLength = recordLength - keyLength;
            if (decompressor == null) {
                // NOTE: this length is not part of the SequenceFile specification, and instead comes from Text readFields
                long expectedValueEnd = input.getPos() + compressedValueLength;
                int textLength = toIntExact(readVInt(input));
                input.readFully(lineBuffer, textLength);
                // verify all value bytes were consumed
                verify(expectedValueEnd == input.getPos(), "Raw value larger than text value");
                return true;
            }

            compressedBuffer.reset();
            input.readFully(compressedBuffer, compressedValueLength);
            Slice compressed = compressedBuffer.slice();

            uncompressedBuffer.reset();
            decompressor.decompress(compressed, uncompressedBuffer);

            // NOTE: this length is not part of the SequenceFile specification, and instead comes from Text readFields
            SliceInput uncompressed = uncompressedBuffer.slice().getInput();
            int textLength = toIntExact(readVInt(uncompressed));
            lineBuffer.write(uncompressed, textLength);
            // verify all value bytes were consumed
            verify(uncompressed.available() == 0, "Raw value larger than text value");
            return true;
        }
    }

    private static class BlockCompressedValueReader
            implements ValueReader
    {
        private static final int INSTANCE_SIZE = instanceSize(BlockCompressedValueReader.class);

        private final String location;
        private final long fileSize;
        private final TrinoDataInputStream input;
        private final long end;

        private final long syncFirst;
        private final long syncSecond;

        private final ReadBuffer valueBuffer;
        private final ReadBuffer lengthsBuffer;

        private ValuesBlock valuesBlock = ValuesBlock.EMPTY_VALUES_BLOCK;

        public BlockCompressedValueReader(
                String location,
                long fileSize,
                TrinoDataInputStream input,
                ValueDecompressor decompressor,
                long end,
                long syncFirst,
                long syncSecond)
        {
            this.location = requireNonNull(location, "location is null");
            this.fileSize = fileSize;
            this.input = requireNonNull(input, "input is null");
            requireNonNull(decompressor, "decompressor is null");
            this.end = end;
            this.syncFirst = syncFirst;
            this.syncSecond = syncSecond;

            this.valueBuffer = new ReadBuffer(input, decompressor);
            this.lengthsBuffer = new ReadBuffer(input, decompressor);
        }

        @Override
        public long getRetainedSize()
        {
            return INSTANCE_SIZE + input.getRetainedSize() + valueBuffer.getRetainedSize() + lengthsBuffer.getRetainedSize() + valuesBlock.getRetainedSize();
        }

        @Override
        public boolean readLine(LineBuffer lineBuffer)
                throws IOException
        {
            if (valuesBlock.readLine(lineBuffer)) {
                return true;
            }
            valuesBlock = ValuesBlock.EMPTY_VALUES_BLOCK;

            // are we at the end?
            if (fileSize - input.getPos() == 0) {
                return false;
            }

            // read uncompressed size of row group (which is useless information)
            verify(fileSize - input.getPos() >= SIZE_OF_INT, "SequenceFile truncated %s", location);
            verify(-1 == input.readInt(), "Invalid sync in SequenceFile %s", location);

            verify(fileSize - input.getPos() >= SIZE_OF_LONG + SIZE_OF_LONG + SIZE_OF_INT, "SequenceFile truncated %s", location);

            // The full sync sequence is "0xFFFFFFFF syncFirst syncSecond".  If
            // this sequence begins in our segment, we must continue process until the
            // next sync sequence.
            // We have already read the 0xFFFFFFFF above, so we must test the
            // end condition back 4 bytes.
            // NOTE: this decision must agree with ReadWriteUtils.findFirstSyncPosition
            if (input.getPos() - SIZE_OF_INT >= end) {
                return false;
            }

            verify(syncFirst == input.readLong() && syncSecond == input.readLong(), "Invalid sync in SequenceFile %s", location);

            // read the block record length
            int blockRecordCount = toIntExact(readVInt(input));
            verify(blockRecordCount > 0, "Invalid block record count %s", blockRecordCount);

            // skip key lengths block
            int keyLengthsLength = readVIntLength("key lengths");
            input.skipNBytes(keyLengthsLength);
            // skip keys block
            int keysLength = readVIntLength("keys");
            input.skipNBytes(keysLength);

            // read value lengths block
            int valueLengthsLength = readVIntLength("value lengths");
            Slice valueLengths = lengthsBuffer.readBlock(valueLengthsLength);
            // read values block
            // the values block could be decompressed in streaming style instead of full block at once
            int valueLength = readVIntLength("values");
            Slice values = valueBuffer.readBlock(valueLength);

            valuesBlock = new ValuesBlock(blockRecordCount, valueLengths.getInput(), values.getInput());
            return valuesBlock.readLine(lineBuffer);
        }

        private int readVIntLength(String name)
                throws IOException
        {
            int blockSize = toIntExact(readVInt(input));
            verify(blockSize >= 0, "Invalid SequenceFile %s: %s block size is negative", location, name);
            return blockSize;
        }

        private static class ValuesBlock
        {
            private static final int INSTANCE_SIZE = instanceSize(ValuesBlock.class);

            public static final ValuesBlock EMPTY_VALUES_BLOCK = new ValuesBlock(0, EMPTY_SLICE.getInput(), EMPTY_SLICE.getInput());

            private final SliceInput lengthsInput;
            private final SliceInput dataInput;
            private int remainingRows;

            public ValuesBlock(int rowCount, SliceInput lengthsInput, SliceInput dataInput)
            {
                checkArgument(rowCount >= 0, "rowCount is negative");
                this.remainingRows = rowCount;
                this.lengthsInput = requireNonNull(lengthsInput, "lengthsInput is null");
                this.dataInput = requireNonNull(dataInput, "dataInput is null");
            }

            public long getRetainedSize()
            {
                return INSTANCE_SIZE + lengthsInput.getRetainedSize() + dataInput.getRetainedSize();
            }

            public boolean readLine(LineBuffer lineBuffer)
                    throws IOException
            {
                if (remainingRows <= 0) {
                    return false;
                }
                remainingRows--;

                int valueLength = toIntExact(readVInt(lengthsInput));
                verify(valueLength >= 0, "Value length is negative");
                // NOTE: this length is not part of the SequenceFile specification, and instead comes from Text readFields
                long expectedValueEnd = dataInput.position() + valueLength;
                int textLength = (int) readVInt(dataInput);
                verify(textLength >= 0 && textLength <= valueLength, "Invalid text length: %s", textLength);
                lineBuffer.write(dataInput, textLength);
                // verify all value bytes were consumed
                verify(expectedValueEnd == dataInput.position(), "Raw value larger than text value");
                return true;
            }
        }
    }

    private static class ReadBuffer
    {
        private static final int INSTANCE_SIZE = instanceSize(ReadBuffer.class);

        private final TrinoDataInputStream input;
        private final ValueDecompressor decompressor;
        private final DynamicSliceOutput compressedBuffer = new DynamicSliceOutput(0);
        private final DynamicSliceOutput uncompressedBuffer = new DynamicSliceOutput(0);

        public ReadBuffer(TrinoDataInputStream input, ValueDecompressor decompressor)
        {
            this.input = requireNonNull(input, "input is null");
            this.decompressor = decompressor;
        }

        public long getRetainedSize()
        {
            return INSTANCE_SIZE + input.getRetainedSize() + compressedBuffer.getRetainedSize() + uncompressedBuffer.getRetainedSize();
        }

        public Slice readBlock(int length)
                throws IOException
        {
            compressedBuffer.reset();
            input.readFully(compressedBuffer, length);
            Slice compressed = compressedBuffer.slice();

            if (decompressor == null) {
                return compressed;
            }

            uncompressedBuffer.reset();
            decompressor.decompress(compressed, uncompressedBuffer);
            return uncompressedBuffer.slice();
        }
    }

    private Slice readLengthPrefixedString(TrinoDataInputStream in)
            throws IOException
    {
        int length = toIntExact(readVInt(in));
        verify(length <= MAX_METADATA_STRING_LENGTH, "Metadata string value is too long (%s) in Sequence File %s", length, location);
        return in.readSlice(length);
    }

    @FormatMethod
    private static void verify(boolean expression, String messageFormat, Object... args)
            throws FileCorruptionException
    {
        if (!expression) {
            throw new FileCorruptionException(messageFormat, args);
        }
    }
}
