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

import com.google.common.collect.ImmutableList;
import io.airlift.http.client.ResponseTooLargeException;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static io.trino.execution.buffer.BufferTestUtils.createPage;
import static io.trino.execution.buffer.BufferTestUtils.serializePage;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_HEADER_SIZE;
import static io.trino.execution.buffer.PagesSerdeUtil.calculateChecksum;
import static io.trino.execution.buffer.PagesSerdeUtil.readSerializedPages;
import static io.trino.server.PagesInputStreamFactory.SERIALIZED_PAGES_MAGIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSerializedPagesDecoder
{
    private static final int RESPONSE_HEADER_SIZE = Integer.BYTES + Long.BYTES + Integer.BYTES;

    @Test
    public void testDecodeAcrossChunkBoundaries()
    {
        List<Slice> pages = ImmutableList.of(
                serializePage(createPage(1)),
                emptyBodyPage(),
                serializePage(createPage(100)),
                serializePage(createPage(7)),
                emptyBodyPage());
        byte[] body = responseBody(pages);
        assertThat(ImmutableList.copyOf(readSerializedPages(Slices.wrappedBuffer(body, RESPONSE_HEADER_SIZE, body.length - RESPONSE_HEADER_SIZE).getInput())))
                .isEqualTo(pages);

        for (int chunkSize : new int[] {1, 2, 3, 7, 11, 13, 16, 17, 64, 1000, body.length}) {
            SerializedPagesDecoder decoder = decode(body, chunkSize);
            assertThat(decoder.finish()).isEqualTo(pages);
            assertThat(decoder.getChecksum()).isEqualTo(calculateChecksum(pages));
            assertThat(decoder.getPagesCount()).isEqualTo(pages.size());
        }
    }

    @Test
    public void testNoPages()
    {
        byte[] body = responseBody(ImmutableList.of());
        for (int chunkSize : new int[] {1, 5, body.length}) {
            SerializedPagesDecoder decoder = decode(body, chunkSize);
            assertThat(decoder.finish()).isEmpty();
            assertThat(decoder.getPagesCount()).isEqualTo(0);
        }
    }

    @Test
    public void testInvalidMagic()
    {
        byte[] body = responseBody(ImmutableList.of(serializePage(createPage(1))));
        body[0] ^= 1;
        assertThatThrownBy(() -> decode(body, 3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Invalid stream header, expected 0x%08x".formatted(SERIALIZED_PAGES_MAGIC));
    }

    @Test
    public void testInvalidPageSize()
    {
        for (int compressedSize : new int[] {-1, Integer.MIN_VALUE, Integer.MAX_VALUE}) {
            byte[] body = responseBody(ImmutableList.of(serializePage(createPage(1))));
            Slices.wrappedBuffer(body).setInt(RESPONSE_HEADER_SIZE + SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET, compressedSize);
            assertThatThrownBy(() -> decode(body, 3))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Invalid serialized page size: " + compressedSize);
        }
    }

    @Test
    public void testTruncatedResponse()
    {
        byte[] body = responseBody(ImmutableList.of(serializePage(createPage(10)), serializePage(createPage(10))));
        int secondPageOffset = RESPONSE_HEADER_SIZE + serializePage(createPage(10)).length();
        int[] truncatedLengths = {
                0,
                RESPONSE_HEADER_SIZE - 1,
                RESPONSE_HEADER_SIZE + 1,
                RESPONSE_HEADER_SIZE + SERIALIZED_PAGE_HEADER_SIZE,
                secondPageOffset + SERIALIZED_PAGE_HEADER_SIZE - 1,
                body.length - 1,
        };
        for (int length : truncatedLengths) {
            SerializedPagesDecoder decoder = decode(Arrays.copyOf(body, length), 4);
            assertThatThrownBy(decoder::finish)
                    .isInstanceOf(UncheckedIOException.class)
                    .hasCauseInstanceOf(EOFException.class);
        }
    }

    @Test
    public void testResponseExceedsContentLength()
    {
        byte[] body = responseBody(ImmutableList.of(serializePage(createPage(10)), serializePage(createPage(10))));
        int secondPageOffset = RESPONSE_HEADER_SIZE + serializePage(createPage(10)).length();
        assertThat(decode(body, 4, body.length).finish()).hasSize(2);
        // the second page header declares a size past the limit before its body arrives
        assertThatThrownBy(() -> decode(Arrays.copyOf(body, secondPageOffset + SERIALIZED_PAGE_HEADER_SIZE), 4, body.length - 1))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(ResponseTooLargeException.class);
    }

    @Test
    public void testResponseHeaderNotRead()
    {
        SerializedPagesDecoder decoder = decode(new byte[RESPONSE_HEADER_SIZE - 1], 1);
        assertThatThrownBy(decoder::getChecksum)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("response header not read");
        assertThatThrownBy(decoder::getPagesCount)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("response header not read");
    }

    private static SerializedPagesDecoder decode(byte[] body, int chunkSize)
    {
        return decode(body, chunkSize, Long.MAX_VALUE);
    }

    private static SerializedPagesDecoder decode(byte[] body, int chunkSize, long maxResponseContentLength)
    {
        SerializedPagesDecoder decoder = new SerializedPagesDecoder(maxResponseContentLength);
        for (int offset = 0; offset < body.length; offset += chunkSize) {
            int length = Math.min(chunkSize, body.length - offset);
            ByteBuffer chunk = ByteBuffer.allocateDirect(length);
            chunk.put(body, offset, length).flip();
            decoder.decode(chunk);
            assertThat(chunk.hasRemaining()).isFalse();
        }
        return decoder;
    }

    private static Slice emptyBodyPage()
    {
        return Slices.allocate(SERIALIZED_PAGE_HEADER_SIZE);
    }

    private static byte[] responseBody(List<Slice> pages)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(1024);
        output.writeInt(SERIALIZED_PAGES_MAGIC);
        output.writeLong(calculateChecksum(pages));
        output.writeInt(pages.size());
        pages.forEach(output::writeBytes);
        return output.slice().getBytes();
    }
}
