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
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;

import static io.trino.block.BlockSerdeUtil.readBlock;
import static io.trino.block.BlockSerdeUtil.writeBlock;
import static io.trino.execution.buffer.PagesSerde.SERIALIZED_PAGE_HEADER_SIZE;
import static io.trino.execution.buffer.PagesSerde.readSerializedPage;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public final class PagesSerdeUtil
{
    private PagesSerdeUtil() {}

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

    public static long writePages(PagesSerde serde, SliceOutput sliceOutput, Page... pages)
    {
        return writePages(serde, sliceOutput, asList(pages).iterator());
    }

    public static long writePages(PagesSerde serde, SliceOutput sliceOutput, Iterator<Page> pages)
    {
        long size = 0;
        try (PagesSerde.PagesSerdeContext context = serde.newContext()) {
            while (pages.hasNext()) {
                Page page = pages.next();
                sliceOutput.writeBytes(serde.serialize(context, page));
                size += page.getSizeInBytes();
            }
        }
        return size;
    }

    public static Iterator<Page> readPages(PagesSerde serde, InputStream inputStream)
    {
        return new PageReader(serde, inputStream);
    }

    private static class PageReader
            extends AbstractIterator<Page>
    {
        private final PagesSerde serde;
        private final PagesSerde.PagesSerdeContext context;
        private final InputStream inputStream;
        private final byte[] headerBuffer = new byte[SERIALIZED_PAGE_HEADER_SIZE];
        private final Slice headerSlice = Slices.wrappedBuffer(headerBuffer);

        PageReader(PagesSerde serde, InputStream inputStream)
        {
            this.serde = requireNonNull(serde, "serde is null");
            this.inputStream = requireNonNull(inputStream, "inputStream is null");
            this.context = serde.newContext();
        }

        @Override
        protected Page computeNext()
        {
            try {
                int read = ByteStreams.read(inputStream, headerBuffer, 0, headerBuffer.length);
                if (read <= 0) {
                    context.close(); // Release context buffers
                    return endOfData();
                }
                else if (read != headerBuffer.length) {
                    throw new EOFException();
                }

                return serde.deserialize(context, readSerializedPage(headerSlice, inputStream));
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
                else if (read != headerBuffer.length) {
                    throw new EOFException();
                }

                return readSerializedPage(headerSlice, inputStream);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
