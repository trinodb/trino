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
package io.trino.execution.buffer;

import com.google.common.collect.AbstractIterator;
import com.google.common.io.ByteStreams;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.trino.execution.buffer.PageCodecMarker.MarkerSet;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.ByteStreams.readFully;
import static io.trino.block.BlockSerdeUtil.readBlock;
import static io.trino.block.BlockSerdeUtil.writeBlock;
import static io.trino.execution.buffer.PageCodecMarker.COMPRESSED;
import static io.trino.execution.buffer.PageCodecMarker.ENCRYPTED;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public final class PagesSerdeUtil
{
    private PagesSerdeUtil() {}

    static final int SERIALIZED_PAGE_POSITION_COUNT_OFFSET = 0;
    static final int SERIALIZED_PAGE_CODEC_MARKERS_OFFSET = SERIALIZED_PAGE_POSITION_COUNT_OFFSET + Integer.BYTES;
    static final int SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET = SERIALIZED_PAGE_CODEC_MARKERS_OFFSET + Byte.BYTES;
    static final int SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET = SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET + Integer.BYTES;
    static final int SERIALIZED_PAGE_HEADER_SIZE = SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET + Integer.BYTES;
    static final String SERIALIZED_PAGE_CIPHER_NAME = "AES/CBC/PKCS5Padding";
    static final int SERIALIZED_PAGE_COMPRESSED_BLOCK_MASK = 1 << (Integer.SIZE - 1);
    static final int ESTIMATED_AES_CIPHER_RETAINED_SIZE = 1024;

    /**
     * Special checksum value used to verify configuration consistency across nodes (all nodes need to have data integrity configured the same way).
     *
     * @implNote It's not just 0, so that hypothetical zero-ed out data is not treated as valid payload with no checksum.
     */
    public static final long NO_CHECKSUM = 0x0123456789abcdefL;

    static void writeRawPage(Page page, SliceOutput output, BlockEncodingSerde serde)
    {
        output.writeInt(page.getChannelCount());
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            writeBlock(serde, output, page.getBlock(channel));
        }
    }

    static Page readRawPage(int positionCount, SliceInput input, BlockEncodingSerde blockEncodingSerde)
    {
        int numberOfBlocks = input.readInt();
        Block[] blocks = new Block[numberOfBlocks];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = readBlock(blockEncodingSerde, input);
        }

        return new Page(positionCount, blocks);
    }

    public static long calculateChecksum(List<Slice> pages)
    {
        XxHash64 hash = new XxHash64();
        for (Slice page : pages) {
            hash.update(page);
        }
        long checksum = hash.hash();
        // Since NO_CHECKSUM is assigned a special meaning, it is not a valid checksum.
        if (checksum == NO_CHECKSUM) {
            return checksum + 1;
        }
        return checksum;
    }

    public static long writePages(PageSerializer serializer, SliceOutput sliceOutput, Page... pages)
    {
        return writePages(serializer, sliceOutput, asList(pages).iterator());
    }

    public static long writePages(PageSerializer serializer, SliceOutput sliceOutput, Iterator<Page> pages)
    {
        long size = 0;
        while (pages.hasNext()) {
            Page page = pages.next();
            sliceOutput.writeBytes(serializer.serialize(page));
            size += page.getSizeInBytes();
        }
        return size;
    }

    public static Iterator<Page> readPages(PageDeserializer deserializer, InputStream inputStream)
    {
        return new PageReader(deserializer, inputStream);
    }

    public static int getSerializedPagePositionCount(Slice serializedPage)
    {
        return serializedPage.getInt(SERIALIZED_PAGE_POSITION_COUNT_OFFSET);
    }

    public static int getSerializedPageUncompressedSizeInBytes(Slice serializedPage)
    {
        return serializedPage.getInt(SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET);
    }

    public static boolean isSerializedPageEncrypted(Slice serializedPage)
    {
        return getSerializedPageMarkerSet(serializedPage).contains(ENCRYPTED);
    }

    public static boolean isSerializedPageCompressed(Slice serializedPage)
    {
        return getSerializedPageMarkerSet(serializedPage).contains(COMPRESSED);
    }

    private static MarkerSet getSerializedPageMarkerSet(Slice serializedPage)
    {
        return MarkerSet.fromByteValue(serializedPage.getByte(Integer.BYTES));
    }

    private static class PageReader
            extends AbstractIterator<Page>
    {
        private final PageDeserializer deserializer;
        private final InputStream inputStream;
        private final byte[] headerBuffer = new byte[SERIALIZED_PAGE_HEADER_SIZE];
        private final Slice headerSlice = Slices.wrappedBuffer(headerBuffer);

        PageReader(PageDeserializer deserializer, InputStream inputStream)
        {
            this.deserializer = requireNonNull(deserializer, "deserializer is null");
            this.inputStream = requireNonNull(inputStream, "inputStream is null");
        }

        @Override
        protected Page computeNext()
        {
            try {
                int read = ByteStreams.read(inputStream, headerBuffer, 0, headerBuffer.length);
                if (read <= 0) {
                    return endOfData();
                }
                if (read != headerBuffer.length) {
                    throw new EOFException();
                }

                return deserializer.deserialize(readSerializedPage(headerSlice, inputStream));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static Iterator<Slice> readSerializedPages(InputStream inputStream)
    {
        return new SerializedPageReader(inputStream);
    }

    private static class SerializedPageReader
            extends AbstractIterator<Slice>
    {
        private final InputStream inputStream;
        private final byte[] headerBuffer = new byte[SERIALIZED_PAGE_HEADER_SIZE];
        private final Slice headerSlice = Slices.wrappedBuffer(headerBuffer);

        SerializedPageReader(InputStream input)
        {
            this.inputStream = requireNonNull(input, "inputStream is null");
        }

        @Override
        protected Slice computeNext()
        {
            try {
                int read = ByteStreams.read(inputStream, headerBuffer, 0, headerBuffer.length);
                if (read <= 0) {
                    return endOfData();
                }
                if (read != headerBuffer.length) {
                    throw new EOFException();
                }

                return readSerializedPage(headerSlice, inputStream);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static Slice readSerializedPage(Slice headerSlice, InputStream inputStream)
            throws IOException
    {
        checkArgument(headerSlice.length() == SERIALIZED_PAGE_HEADER_SIZE, "headerSlice length should equal to %s", SERIALIZED_PAGE_HEADER_SIZE);

        int compressedSize = headerSlice.getIntUnchecked(SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET);
        byte[] outputBuffer = new byte[SERIALIZED_PAGE_HEADER_SIZE + compressedSize];
        headerSlice.getBytes(0, outputBuffer, 0, SERIALIZED_PAGE_HEADER_SIZE);
        readFully(inputStream, outputBuffer, SERIALIZED_PAGE_HEADER_SIZE, compressedSize);
        return Slices.wrappedBuffer(outputBuffer);
    }
}
