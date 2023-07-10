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

import com.google.common.io.Closer;
import com.google.common.io.CountingOutputStream;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.hive.formats.DataOutputStream;
import io.trino.hive.formats.compression.Codec;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.compression.MemoryCompressedSliceOutput;
import io.trino.hive.formats.compression.ValueCompressor;
import io.trino.hive.formats.line.LineWriter;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.hive.formats.ReadWriteUtils.computeVIntLength;
import static io.trino.hive.formats.ReadWriteUtils.writeLengthPrefixedString;
import static io.trino.hive.formats.ReadWriteUtils.writeVInt;
import static io.trino.hive.formats.compression.CompressionKind.LZOP;
import static java.util.Objects.requireNonNull;

/**
 * Sequence file writer is hard coded to the behavior of HiveSequenceFileOutputFormat with a Text value class.
 */
public class SequenceFileWriter
        implements LineWriter
{
    private static final int INSTANCE_SIZE = instanceSize(SequenceFileWriter.class);
    private static final Slice SEQUENCE_FILE_MAGIC = utf8Slice("SEQ");
    private static final byte SEQUENCE_FILE_VERSION = 6;

    static final String TRINO_SEQUENCE_FILE_WRITER_VERSION_METADATA_KEY = "trino.writer.version";
    static final String TRINO_SEQUENCE_FILE_WRITER_VERSION;

    static {
        String version = SequenceFileWriter.class.getPackage().getImplementationVersion();
        TRINO_SEQUENCE_FILE_WRITER_VERSION = version == null ? "UNKNOWN" : version;
    }

    private final ValueWriter valueWriter;
    private final LongSupplier writtenBytes;

    public SequenceFileWriter(
            OutputStream rawOutput,
            Optional<CompressionKind> compressionKind,
            boolean blockCompression,
            Map<String, String> metadata)
            throws IOException
    {
        try {
            requireNonNull(rawOutput, "raw output is null");
            requireNonNull(compressionKind, "compressionKind is null");
            checkArgument(!blockCompression || compressionKind.isPresent(), "Block compression can only be enabled when a compression codec is provided");
            checkArgument(!compressionKind.equals(Optional.of(LZOP)), "LZOP cannot be used with SequenceFile. LZO compression can be used, but LZ4 is preferred.");

            CountingOutputStream countingOutputStream = new CountingOutputStream(rawOutput);
            writtenBytes = countingOutputStream::getCount;
            DataOutputStream output = new DataOutputStream(countingOutputStream);

            // write magic with version
            output.write(SEQUENCE_FILE_MAGIC);
            output.write(SEQUENCE_FILE_VERSION);

            // write key and value class names
            writeLengthPrefixedString(output, utf8Slice("org.apache.hadoop.io.BytesWritable"));
            writeLengthPrefixedString(output, utf8Slice("org.apache.hadoop.io.Text"));

            // write compression info
            output.writeBoolean(compressionKind.isPresent());
            output.writeBoolean(blockCompression);
            if (compressionKind.isPresent()) {
                writeLengthPrefixedString(output, utf8Slice(compressionKind.get().getHadoopClassName()));
            }

            // write metadata
            output.writeInt(Integer.reverseBytes(metadata.size() + 1));
            writeMetadataProperty(output, TRINO_SEQUENCE_FILE_WRITER_VERSION_METADATA_KEY, TRINO_SEQUENCE_FILE_WRITER_VERSION);
            for (Entry<String, String> entry : metadata.entrySet()) {
                writeMetadataProperty(output, entry.getKey(), entry.getValue());
            }

            // write sync value
            long syncFirst = ThreadLocalRandom.current().nextLong();
            long syncSecond = ThreadLocalRandom.current().nextLong();
            output.writeLong(syncFirst);
            output.writeLong(syncSecond);

            Optional<Codec> codec = compressionKind.map(CompressionKind::createCodec);
            if (blockCompression) {
                valueWriter = new BlockCompressionValueWriter(output, codec.orElseThrow(), syncFirst, syncSecond);
            }
            else {
                valueWriter = new SingleValueWriter(output, codec, syncFirst, syncSecond);
            }
        }
        catch (Throwable e) {
            try (rawOutput) {
                throw e;
            }
        }
    }

    private static void writeMetadataProperty(DataOutputStream output, String key, String value)
            throws IOException
    {
        writeLengthPrefixedString(output, utf8Slice(key));
        writeLengthPrefixedString(output, utf8Slice(value));
    }

    @Override
    public long getWrittenBytes()
    {
        return writtenBytes.getAsLong();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + valueWriter.getRetainedSizeInBytes();
    }

    @Override
    public void write(Slice value)
            throws IOException
    {
        valueWriter.write(value);
    }

    @Override
    public void close()
            throws IOException
    {
        valueWriter.close();
    }

    private interface ValueWriter
            extends Closeable
    {
        void write(Slice value)
                throws IOException;

        long getRetainedSizeInBytes();
    }

    private static class SingleValueWriter
            implements ValueWriter
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleValueWriter.class);
        private static final int SYNC_INTERVAL = 10 * 1024;

        private final DataOutputStream output;
        private final long syncFirst;
        private final long syncSecond;
        private final ValueCompressor valueCompressor;
        private final DynamicSliceOutput uncompressedBuffer = new DynamicSliceOutput(0);

        // SliceOutput does not have a long position, so track it manually
        private long currentPosition;
        private long lastSyncPosition;

        private final Closer closer = Closer.create();

        public SingleValueWriter(DataOutputStream output, Optional<Codec> codec, long syncFirst, long syncSecond)
                throws IOException
        {
            try {
                this.output = closer.register(requireNonNull(output, "output is null"));
                requireNonNull(codec, "codec is null");
                if (codec.isPresent()) {
                    this.valueCompressor = codec.get().createValueCompressor();
                }
                else {
                    valueCompressor = null;
                }
                this.syncFirst = syncFirst;
                this.syncSecond = syncSecond;
                currentPosition += output.longSize();
            }
            catch (Throwable e) {
                try (closer) {
                    throw e;
                }
            }
        }

        @Override
        public void write(Slice value)
                throws IOException
        {
            try {
                // prefix the value with the vInt length before compression
                // NOTE: this length is not part of the SequenceFile specification, and instead comes from Text readFields
                uncompressedBuffer.reset();
                writeVInt(uncompressedBuffer, value.length());
                uncompressedBuffer.writeBytes(value);
                value = uncompressedBuffer.slice();

                Slice compressedValue;
                if (valueCompressor == null) {
                    compressedValue = value;
                }
                else {
                    compressedValue = valueCompressor.compress(value);
                }

                // total entry length = 4 bytes for zero key + compressed value
                output.writeInt(Integer.reverseBytes(4 + compressedValue.length()));
                output.writeInt(Integer.reverseBytes(4));
                output.writeInt(0);
                output.write(compressedValue);
                currentPosition += SIZE_OF_INT + SIZE_OF_INT + SIZE_OF_INT + compressedValue.length();

                if (output.longSize() - lastSyncPosition > SYNC_INTERVAL) {
                    output.writeInt(-1);
                    output.writeLong(syncFirst);
                    output.writeLong(syncSecond);
                    currentPosition += SIZE_OF_INT + SIZE_OF_LONG + SIZE_OF_LONG;
                    lastSyncPosition = currentPosition;
                }
            }
            catch (Throwable e) {
                try (closer) {
                    throw e;
                }
            }
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + output.getRetainedSize();
        }

        @Override
        public void close()
                throws IOException
        {
            closer.close();
        }
    }

    private static class BlockCompressionValueWriter
            implements ValueWriter
    {
        private static final int INSTANCE_SIZE = instanceSize(BlockCompressionValueWriter.class);

        private static final int MAX_ROWS = 10_000_000;
        private static final int TARGET_BLOCK_SIZE = 1024 * 1024;

        private static final int OTHER_MIN_BUFFER_SIZE = 4 * 1024;
        private static final int OTHER_MAX_BUFFER_SIZE = 16 * 1024;

        private static final int VALUE_MIN_BUFFER_SIZE = 4 * 1024;
        private static final int VALUE_MAX_BUFFER_SIZE = 1024 * 1024;

        private final DataOutputStream output;
        private final long syncFirst;
        private final long syncSecond;

        private MemoryCompressedSliceOutput keyLengthOutput;
        private MemoryCompressedSliceOutput keyOutput;
        private MemoryCompressedSliceOutput valueLengthOutput;
        private MemoryCompressedSliceOutput valueOutput;

        private int valueCount;

        private final Closer closer = Closer.create();

        public BlockCompressionValueWriter(DataOutputStream output, Codec trinoCodec, long syncFirst, long syncSecond)
                throws IOException
        {
            try {
                this.output = closer.register(requireNonNull(output, "output is null"));
                requireNonNull(trinoCodec, "trinoCodec is null");
                this.syncFirst = syncFirst;
                this.syncSecond = syncSecond;

                this.keyLengthOutput = trinoCodec.createMemoryCompressedSliceOutput(OTHER_MIN_BUFFER_SIZE, OTHER_MAX_BUFFER_SIZE);
                closer.register(keyLengthOutput::destroy);
                this.keyOutput = trinoCodec.createMemoryCompressedSliceOutput(OTHER_MIN_BUFFER_SIZE, OTHER_MAX_BUFFER_SIZE);
                closer.register(keyOutput::destroy);

                this.valueLengthOutput = trinoCodec.createMemoryCompressedSliceOutput(OTHER_MIN_BUFFER_SIZE, OTHER_MAX_BUFFER_SIZE);
                closer.register(valueLengthOutput::destroy);
                this.valueOutput = trinoCodec.createMemoryCompressedSliceOutput(VALUE_MIN_BUFFER_SIZE, VALUE_MAX_BUFFER_SIZE);
                closer.register(valueOutput::destroy);
            }
            catch (Throwable e) {
                try (closer) {
                    throw e;
                }
            }
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE +
                    output.getRetainedSize() +
                    keyLengthOutput.getRetainedSize() +
                    keyOutput.getRetainedSize() +
                    valueLengthOutput.getRetainedSize() +
                    valueOutput.getRetainedSize();
        }

        @Override
        public void close()
                throws IOException
        {
            flushBlock();

            closer.close();
        }

        @Override
        public void write(Slice value)
                throws IOException
        {
            try {
                // keys are always 4 bytes of zero
                keyLengthOutput.writeInt(4);
                keyOutput.writeInt(0);

                // write value
                // Full length of the value including the vInt added by text writable
                writeVInt(valueLengthOutput, value.length() + computeVIntLength(value.length()));
                // NOTE: this length is not part of the SequenceFile specification, and instead comes from Text readFields
                writeVInt(valueOutput, value.length());
                valueOutput.writeBytes(value);

                valueCount++;

                if (valueCount >= MAX_ROWS || getBufferedSize() > TARGET_BLOCK_SIZE) {
                    flushBlock();
                }
            }
            catch (Throwable e) {
                try (closer) {
                    throw e;
                }
            }
        }

        private int getBufferedSize()
        {
            return keyLengthOutput.size() +
                    keyOutput.size() +
                    valueLengthOutput.size() +
                    valueOutput.size();
        }

        private void flushBlock()
                throws IOException
        {
            if (valueCount == 0) {
                return;
            }

            // write sync
            output.writeInt(-1);
            output.writeLong(syncFirst);
            output.writeLong(syncSecond);

            // write block data and recycle buffers
            writeVInt(output, valueCount);
            keyLengthOutput = writeBlockAndRecycleBuffer(keyLengthOutput);
            keyOutput = writeBlockAndRecycleBuffer(keyOutput);
            valueLengthOutput = writeBlockAndRecycleBuffer(valueLengthOutput);
            valueOutput = writeBlockAndRecycleBuffer(valueOutput);

            valueCount = 0;
        }

        private MemoryCompressedSliceOutput writeBlockAndRecycleBuffer(MemoryCompressedSliceOutput compressedBlock)
                throws IOException
        {
            compressedBlock.close();
            writeVInt(output, compressedBlock.getCompressedSize());
            for (Slice slice : compressedBlock.getCompressedSlices()) {
                output.write(slice);
            }
            return compressedBlock.createRecycledCompressedSliceOutput();
        }
    }
}
