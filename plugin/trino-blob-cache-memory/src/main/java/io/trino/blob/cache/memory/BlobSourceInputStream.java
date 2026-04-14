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
package io.trino.blob.cache.memory;

import io.trino.spi.cache.BlobSource;

import java.io.IOException;
import java.io.InputStream;

import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class BlobSourceInputStream
        extends InputStream
{
    private final BlobSource source;
    private final long length;
    private long position;

    BlobSourceInputStream(BlobSource source, long length)
    {
        this.source = requireNonNull(source, "source is null");
        this.length = length;
    }

    @Override
    public int read()
            throws IOException
    {
        if (position >= length) {
            return -1;
        }
        byte[] single = new byte[1];
        source.readFully(position, single, 0, 1);
        position++;
        return single[0] & 0xFF;
    }

    @Override
    public int read(byte[] buffer, int offset, int len)
            throws IOException
    {
        checkFromIndexSize(offset, len, buffer.length);
        if (len == 0) {
            return 0;
        }
        if (position >= length) {
            return -1;
        }
        int toRead = toIntBounded(min(len, length - position));
        source.readFully(position, buffer, offset, toRead);
        position += toRead;
        return toRead;
    }

    @Override
    public long skip(long n)
    {
        if (n <= 0) {
            return 0;
        }
        long skipped = min(n, length - position);
        position += skipped;
        return skipped;
    }

    @Override
    public int available()
    {
        return toIntBounded(length - position);
    }

    private static int toIntBounded(long value)
    {
        return (value > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) value;
    }
}
