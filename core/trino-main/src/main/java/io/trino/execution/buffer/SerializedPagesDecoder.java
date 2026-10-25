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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.EOFException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_HEADER_SIZE;
import static io.trino.server.PagesInputStreamFactory.SERIALIZED_PAGES_MAGIC;
import static java.lang.String.format;

/**
 * Decodes a serialized pages response from body chunks as they arrive, copying each page once into its own array.
 * A page whose declared size takes the response past the content length limit fails before its array is allocated.
 */
public class SerializedPagesDecoder
{
    private static final int CHECKSUM_OFFSET = Integer.BYTES;
    private static final int PAGES_COUNT_OFFSET = CHECKSUM_OFFSET + Long.BYTES;
    private static final int RESPONSE_HEADER_SIZE = PAGES_COUNT_OFFSET + Integer.BYTES;

    private final long maxResponseContentLength;
    private final byte[] responseHeader = new byte[RESPONSE_HEADER_SIZE];
    private final byte[] pageHeader = new byte[SERIALIZED_PAGE_HEADER_SIZE];
    private final ImmutableList.Builder<Slice> pages = ImmutableList.builder();

    private boolean responseHeaderRead;
    private long checksum;
    private int pagesCount;
    private long responseContentLength;
    private int headerPosition;
    private byte[] page;
    private int pagePosition;

    public SerializedPagesDecoder(long maxResponseContentLength)
    {
        checkArgument(maxResponseContentLength > 0, "maxResponseContentLength must be positive: %s", maxResponseContentLength);
        this.maxResponseContentLength = maxResponseContentLength;
    }

    /**
     * Consumes all remaining bytes of the content.
     */
    public void decode(ByteBuffer content)
    {
        while (content.hasRemaining()) {
            if (!responseHeaderRead) {
                if (fillHeader(responseHeader, content)) {
                    readResponseHeader();
                }
                continue;
            }
            if (page == null) {
                if (!fillHeader(pageHeader, content)) {
                    return;
                }
                int compressedSize = Slices.wrappedBuffer(pageHeader).getInt(SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET);
                checkState(compressedSize >= 0 && compressedSize <= Integer.MAX_VALUE - SERIALIZED_PAGE_HEADER_SIZE, "Invalid serialized page size: %s", compressedSize);
                responseContentLength += SERIALIZED_PAGE_HEADER_SIZE + compressedSize;
                if (responseContentLength > maxResponseContentLength) {
                    throw new UncheckedIOException(new ResponseTooLargeException(format("Serialized pages response of at least %s bytes exceeds the limit of %s bytes", responseContentLength, maxResponseContentLength)));
                }
                page = new byte[SERIALIZED_PAGE_HEADER_SIZE + compressedSize];
                System.arraycopy(pageHeader, 0, page, 0, SERIALIZED_PAGE_HEADER_SIZE);
                pagePosition = SERIALIZED_PAGE_HEADER_SIZE;
            }
            int count = Math.min(content.remaining(), page.length - pagePosition);
            content.get(page, pagePosition, count);
            pagePosition += count;
            if (pagePosition == page.length) {
                pages.add(Slices.wrappedBuffer(page));
                page = null;
            }
        }
    }

    private boolean fillHeader(byte[] header, ByteBuffer content)
    {
        int count = Math.min(content.remaining(), header.length - headerPosition);
        content.get(header, headerPosition, count);
        headerPosition += count;
        if (headerPosition < header.length) {
            return false;
        }
        headerPosition = 0;
        return true;
    }

    private void readResponseHeader()
    {
        Slice header = Slices.wrappedBuffer(responseHeader);
        int magic = header.getInt(0);
        if (magic != SERIALIZED_PAGES_MAGIC) {
            throw new IllegalStateException(format("Invalid stream header, expected 0x%08x, but was 0x%08x", SERIALIZED_PAGES_MAGIC, magic));
        }
        checksum = header.getLong(CHECKSUM_OFFSET);
        pagesCount = header.getInt(PAGES_COUNT_OFFSET);
        responseContentLength = RESPONSE_HEADER_SIZE;
        responseHeaderRead = true;
    }

    /**
     * Returns the decoded pages after the whole response has been consumed.
     */
    public List<Slice> finish()
    {
        if (!responseHeaderRead || headerPosition > 0 || page != null) {
            throw new UncheckedIOException(new EOFException("Serialized pages response ended in the middle of a header or page"));
        }
        return pages.build();
    }

    public long getChecksum()
    {
        checkState(responseHeaderRead, "response header not read");
        return checksum;
    }

    public int getPagesCount()
    {
        checkState(responseHeaderRead, "response header not read");
        return pagesCount;
    }
}
