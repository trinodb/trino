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
package io.trino.server.protocol.spooling.encoding;

import io.trino.client.spooling.DataAttributes;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.spi.Page;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static com.google.common.primitives.Ints.saturatedCast;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.client.spooling.DataAttribute.UNCOMPRESSED_SIZE;

public abstract class CompressedQueryDataEncoder
        implements QueryDataEncoder
{
    protected final QueryDataEncoder delegate;
    private final int compressionThreshold;

    protected CompressedQueryDataEncoder(QueryDataEncoder delegate, int compressionThreshold)
    {
        this.delegate = delegate;
        this.compressionThreshold = compressionThreshold;
    }

    @Override
    public DataAttributes encodeTo(OutputStream output, List<Page> pages)
            throws IOException
    {
        ExposedByteArrayOutputStream buffer = new ExposedByteArrayOutputStream(pagesSize(pages));
        DataAttributes attributes = delegate.encodeTo(buffer, pages);
        int uncompressedSize = attributes.get(SEGMENT_SIZE, Integer.class);

        // Do not compress data if below threshold
        if (uncompressedSize < compressionThreshold) {
            buffer.writeTo(output);
            return attributes;
        }

        return attributes
                .toBuilder()
                // Compress directly from the backing array (only the first uncompressedSize bytes are read) to avoid copying the whole segment
                .set(SEGMENT_SIZE, compress(buffer.buffer(), uncompressedSize, output)) // actual size of compressed data
                .set(UNCOMPRESSED_SIZE, uncompressedSize) // expected by the decoder if the data is compressed
                .build();
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    protected abstract int compress(byte[] buffer, int uncompressedSize, OutputStream output)
            throws IOException;

    protected static int pagesSize(List<Page> pages)
    {
        return saturatedCast(pages.stream()
                .mapToLong(Page::getSizeInBytes)
                .sum());
    }

    // Exposes the backing array to avoid the defensive copy made by ByteArrayOutputStream.toByteArray().
    // Safe because the buffer is consumed on the same thread immediately after encoding completes.
    private static final class ExposedByteArrayOutputStream
            extends ByteArrayOutputStream
    {
        public ExposedByteArrayOutputStream(int size)
        {
            super(size);
        }

        public byte[] buffer()
        {
            return buf;
        }
    }
}
