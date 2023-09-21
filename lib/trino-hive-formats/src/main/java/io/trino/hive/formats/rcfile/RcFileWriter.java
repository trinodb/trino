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

import com.google.common.io.Closer;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.DataOutputStream;
import io.trino.hive.formats.FileCorruptionException;
import io.trino.hive.formats.compression.Codec;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.compression.MemoryCompressedSliceOutput;
import io.trino.hive.formats.encodings.ColumnEncoding;
import io.trino.hive.formats.encodings.ColumnEncodingFactory;
import io.trino.hive.formats.encodings.EncodeOutput;
import io.trino.hive.formats.rcfile.RcFileWriteValidation.RcFileWriteValidationBuilder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.hive.formats.ReadWriteUtils.writeLengthPrefixedString;
import static io.trino.hive.formats.ReadWriteUtils.writeVInt;
import static io.trino.hive.formats.compression.CompressionKind.LZOP;
import static io.trino.hive.formats.rcfile.RcFileReader.validateFile;
import static java.lang.StrictMath.toIntExact;
import static java.util.Objects.requireNonNull;

public class RcFileWriter
        implements Closeable
{
    private static final int INSTANCE_SIZE = instanceSize(RcFileWriter.class);
    private static final Slice RCFILE_MAGIC = utf8Slice("RCF");
    private static final int CURRENT_VERSION = 1;
    private static final String COLUMN_COUNT_METADATA_KEY = "hive.io.rcfile.column.number";
    private static final DataSize DEFAULT_TARGET_MIN_ROW_GROUP_SIZE = DataSize.of(4, MEGABYTE);
    private static final DataSize DEFAULT_TARGET_MAX_ROW_GROUP_SIZE = DataSize.of(8, MEGABYTE);
    private static final DataSize MIN_BUFFER_SIZE = DataSize.of(4, KILOBYTE);
    private static final DataSize MAX_BUFFER_SIZE = DataSize.of(1, MEGABYTE);

    static final String PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY = "presto.writer.version";
    static final String PRESTO_RCFILE_WRITER_VERSION;

    static {
        String version = RcFileWriter.class.getPackage().getImplementationVersion();
        PRESTO_RCFILE_WRITER_VERSION = version == null ? "UNKNOWN" : version;
    }

    private final DataOutputStream output;
    private final List<Type> types;
    private final ColumnEncodingFactory encoding;

    private final long syncFirst = ThreadLocalRandom.current().nextLong();
    private final long syncSecond = ThreadLocalRandom.current().nextLong();

    private MemoryCompressedSliceOutput keySectionOutput;
    private final ColumnEncoder[] columnEncoders;

    private final int targetMinRowGroupSize;
    private final int targetMaxRowGroupSize;

    private int bufferedSize;
    private int bufferedRows;

    private long totalRowCount;

    @Nullable
    private final RcFileWriteValidationBuilder validationBuilder;

    public RcFileWriter(
            OutputStream rawOutput,
            List<Type> types,
            ColumnEncodingFactory encoding,
            Optional<CompressionKind> compressionKind,
            Map<String, String> metadata,
            boolean validate)
            throws IOException
    {
        this(
                rawOutput,
                types,
                encoding,
                compressionKind,
                metadata,
                DEFAULT_TARGET_MIN_ROW_GROUP_SIZE,
                DEFAULT_TARGET_MAX_ROW_GROUP_SIZE,
                validate);
    }

    public RcFileWriter(
            OutputStream rawOutput,
            List<Type> types,
            ColumnEncodingFactory encoding,
            Optional<CompressionKind> compressionKind,
            Map<String, String> metadata,
            DataSize targetMinRowGroupSize,
            DataSize targetMaxRowGroupSize,
            boolean validate)
            throws IOException
    {
        requireNonNull(rawOutput, "rawOutput is null");
        requireNonNull(types, "types is null");
        checkArgument(!types.isEmpty(), "types is empty");
        requireNonNull(encoding, "encoding is null");
        requireNonNull(compressionKind, "compressionKind is null");
        checkArgument(!compressionKind.equals(Optional.of(LZOP)), "LZOP cannot be use with RCFile.  LZO compression can be used, but LZ4 is preferred.");
        requireNonNull(metadata, "metadata is null");
        checkArgument(!metadata.containsKey(PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY), "Cannot set property %s", PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY);
        checkArgument(!metadata.containsKey(COLUMN_COUNT_METADATA_KEY), "Cannot set property %s", COLUMN_COUNT_METADATA_KEY);
        requireNonNull(targetMinRowGroupSize, "targetMinRowGroupSize is null");
        requireNonNull(targetMaxRowGroupSize, "targetMaxRowGroupSize is null");
        checkArgument(targetMinRowGroupSize.compareTo(targetMaxRowGroupSize) <= 0, "targetMinRowGroupSize must be less than or equal to targetMaxRowGroupSize");

        this.validationBuilder = validate ? new RcFileWriteValidationBuilder(types) : null;

        this.output = new DataOutputStream(rawOutput);
        this.types = types;
        this.encoding = encoding;

        // write header
        output.write(RCFILE_MAGIC);
        output.writeByte(CURRENT_VERSION);
        recordValidation(validation -> validation.setVersion((byte) CURRENT_VERSION));

        // write codec information
        output.writeBoolean(compressionKind.isPresent());
        if (compressionKind.isPresent()) {
            writeLengthPrefixedString(output, utf8Slice(compressionKind.get().getHadoopClassName()));
        }
        recordValidation(validation -> validation.setCodecClassName(compressionKind.map(CompressionKind::getHadoopClassName)));

        // write metadata
        output.writeInt(Integer.reverseBytes(metadata.size() + 2));
        writeMetadataProperty(COLUMN_COUNT_METADATA_KEY, Integer.toString(types.size()));
        writeMetadataProperty(PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY, PRESTO_RCFILE_WRITER_VERSION);
        for (Entry<String, String> entry : metadata.entrySet()) {
            writeMetadataProperty(entry.getKey(), entry.getValue());
        }

        // write sync sequence
        output.writeLong(syncFirst);
        recordValidation(validation -> validation.setSyncFirst(syncFirst));
        output.writeLong(syncSecond);
        recordValidation(validation -> validation.setSyncSecond(syncSecond));

        // initialize columns
        Optional<Codec> codec = compressionKind.map(CompressionKind::createCodec);
        keySectionOutput = createMemoryCompressedSliceOutput(codec);
        keySectionOutput.close(); // output is recycled on first use which requires the output to be closed
        columnEncoders = new ColumnEncoder[types.size()];
        for (int columnIndex = 0; columnIndex < types.size(); columnIndex++) {
            Type type = types.get(columnIndex);
            ColumnEncoding columnEncoding = encoding.getEncoding(type);
            columnEncoders[columnIndex] = new ColumnEncoder(columnEncoding, codec);
        }
        this.targetMinRowGroupSize = toIntExact(targetMinRowGroupSize.toBytes());
        this.targetMaxRowGroupSize = toIntExact(targetMaxRowGroupSize.toBytes());
    }

    private void writeMetadataProperty(String key, String value)
            throws IOException
    {
        writeLengthPrefixedString(output, utf8Slice(key));
        writeLengthPrefixedString(output, utf8Slice(value));
        recordValidation(validation -> validation.addMetadataProperty(key, value));
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(output);
            closer.register(keySectionOutput::destroy);
            for (ColumnEncoder columnEncoder : columnEncoders) {
                closer.register(columnEncoder::destroy);
            }
            writeRowGroup();
        }
    }

    private void recordValidation(Consumer<RcFileWriteValidationBuilder> task)
    {
        if (validationBuilder != null) {
            task.accept(validationBuilder);
        }
    }

    public void validate(TrinoInputFile inputFile)
            throws FileCorruptionException
    {
        checkState(validationBuilder != null, "validation is not enabled");
        validateFile(
                validationBuilder.build(),
                inputFile,
                encoding,
                types);
    }

    public long getRetainedSizeInBytes()
    {
        long retainedSize = INSTANCE_SIZE;
        retainedSize += output.getRetainedSize();
        retainedSize += keySectionOutput.getRetainedSize();
        for (ColumnEncoder columnEncoder : columnEncoders) {
            retainedSize += columnEncoder.getRetainedSizeInBytes();
        }
        return retainedSize;
    }

    public void write(Page page)
            throws IOException
    {
        if (page.getPositionCount() == 0) {
            return;
        }
        List<Page> pages = PageSplitterUtil.splitPage(page, targetMaxRowGroupSize);
        for (Page splitPage : pages) {
            bufferPage(splitPage);
        }
    }

    private void bufferPage(Page page)
            throws IOException
    {
        bufferedRows += page.getPositionCount();

        bufferedSize = 0;
        for (int i = 0; i < page.getChannelCount(); i++) {
            Block block = page.getBlock(i);
            columnEncoders[i].writeBlock(block);
            bufferedSize += columnEncoders[i].getBufferedSize();
        }
        recordValidation(validation -> validation.addPage(page));

        if (bufferedSize >= targetMinRowGroupSize) {
            writeRowGroup();
        }
    }

    private void writeRowGroup()
            throws IOException
    {
        if (bufferedRows == 0) {
            return;
        }

        // write sync sequence for every row group except for the first row group
        if (totalRowCount != 0) {
            output.writeInt(-1);
            output.writeLong(syncFirst);
            output.writeLong(syncSecond);
        }

        // flush and compress each column
        for (ColumnEncoder columnEncoder : columnEncoders) {
            columnEncoder.closeColumn();
        }

        // build key section
        int valueLength = 0;
        keySectionOutput = keySectionOutput.createRecycledCompressedSliceOutput();
        try {
            writeVInt(keySectionOutput, bufferedRows);
            recordValidation(validation -> validation.addRowGroup(bufferedRows));
            for (ColumnEncoder columnEncoder : columnEncoders) {
                valueLength += columnEncoder.getCompressedSize();
                writeVInt(keySectionOutput, columnEncoder.getCompressedSize());
                writeVInt(keySectionOutput, columnEncoder.getUncompressedSize());

                Slice lengthData = columnEncoder.getLengthData();
                writeVInt(keySectionOutput, lengthData.length());
                keySectionOutput.writeBytes(lengthData);
            }
        }
        finally {
            keySectionOutput.close();
        }

        // write the sum of the uncompressed key length and compressed value length
        // this number is useless to the reader
        output.writeInt(Integer.reverseBytes(keySectionOutput.size() + valueLength));

        // write key section
        output.writeInt(Integer.reverseBytes(keySectionOutput.size()));
        output.writeInt(Integer.reverseBytes(keySectionOutput.getCompressedSize()));
        for (Slice slice : keySectionOutput.getCompressedSlices()) {
            output.write(slice);
        }

        // write value section
        for (ColumnEncoder columnEncoder : columnEncoders) {
            List<Slice> slices = columnEncoder.getCompressedData();
            for (Slice slice : slices) {
                output.write(slice);
            }
            columnEncoder.reset();
        }

        totalRowCount += bufferedRows;
        bufferedSize = 0;
        bufferedRows = 0;
    }

    private static MemoryCompressedSliceOutput createMemoryCompressedSliceOutput(Optional<Codec> codec)
            throws IOException
    {
        if (codec.isPresent()) {
            return codec.get().createMemoryCompressedSliceOutput((int) MIN_BUFFER_SIZE.toBytes(), (int) MAX_BUFFER_SIZE.toBytes());
        }
        return MemoryCompressedSliceOutput.createUncompressedMemorySliceOutput((int) MIN_BUFFER_SIZE.toBytes(), (int) MAX_BUFFER_SIZE.toBytes());
    }

    private static class ColumnEncoder
    {
        private static final int INSTANCE_SIZE = instanceSize(ColumnEncoder.class) + instanceSize(ColumnEncodeOutput.class);

        private final ColumnEncoding columnEncoding;

        private ColumnEncodeOutput encodeOutput;

        private final SliceOutput lengthOutput = new DynamicSliceOutput(512);

        private MemoryCompressedSliceOutput output;

        private boolean columnClosed;

        public ColumnEncoder(ColumnEncoding columnEncoding, Optional<Codec> codec)
                throws IOException
        {
            this.columnEncoding = columnEncoding;
            this.output = createMemoryCompressedSliceOutput(codec);
            this.encodeOutput = new ColumnEncodeOutput(lengthOutput, output);
        }

        private void writeBlock(Block block)
                throws IOException
        {
            checkArgument(!columnClosed, "Column is closed");
            columnEncoding.encodeColumn(block, output, encodeOutput);
        }

        public void closeColumn()
                throws IOException
        {
            checkArgument(!columnClosed, "Column is not open");

            encodeOutput.flush();
            output.close();

            columnClosed = true;
        }

        public int getBufferedSize()
        {
            return lengthOutput.size() + output.size();
        }

        public Slice getLengthData()
        {
            checkArgument(columnClosed, "Column is open");
            return lengthOutput.slice();
        }

        public int getUncompressedSize()
        {
            checkArgument(columnClosed, "Column is open");
            return output.size();
        }

        public int getCompressedSize()
        {
            checkArgument(columnClosed, "Column is open");
            return output.getCompressedSize();
        }

        public List<Slice> getCompressedData()
        {
            checkArgument(columnClosed, "Column is open");
            return output.getCompressedSlices();
        }

        public void reset()
                throws IOException
        {
            checkArgument(columnClosed, "Column is open");
            lengthOutput.reset();

            output = output.createRecycledCompressedSliceOutput();
            encodeOutput = new ColumnEncodeOutput(lengthOutput, output);

            columnClosed = false;
        }

        public void destroy()
                throws IOException
        {
            output.destroy();
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + lengthOutput.getRetainedSize() + output.getRetainedSize();
        }

        private static class ColumnEncodeOutput
                implements EncodeOutput
        {
            private final SliceOutput lengthOutput;
            private final SliceOutput valueOutput;
            private int previousOffset;
            private int previousLength;
            private int runLength;

            public ColumnEncodeOutput(SliceOutput lengthOutput, SliceOutput valueOutput)
            {
                this.lengthOutput = lengthOutput;
                this.valueOutput = valueOutput;
                this.previousOffset = valueOutput.size();
                this.previousLength = -1;
            }

            @Override
            public void closeEntry()
            {
                int offset = valueOutput.size();
                int length = offset - previousOffset;
                previousOffset = offset;

                if (length == previousLength) {
                    runLength++;
                }
                else {
                    if (runLength > 0) {
                        int value = ~runLength;
                        writeVInt(lengthOutput, value);
                    }
                    writeVInt(lengthOutput, length);
                    previousLength = length;
                    runLength = 0;
                }
            }

            private void flush()
            {
                if (runLength > 0) {
                    int value = ~runLength;
                    writeVInt(lengthOutput, value);
                }
                previousLength = -1;
                runLength = 0;
            }
        }
    }
}
