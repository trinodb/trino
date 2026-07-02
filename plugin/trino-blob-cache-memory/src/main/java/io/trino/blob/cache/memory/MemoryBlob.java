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

import io.airlift.slice.Slice;
import io.trino.spi.cache.Blob;

import java.io.EOFException;
import java.io.IOException;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class MemoryBlob
        implements Blob
{
    private final Slice data;

    MemoryBlob(Slice data)
    {
        this.data = requireNonNull(data, "data is null");
    }

    @Override
    public long length()
    {
        return data.length();
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        checkFromIndexSize(offset, length, buffer.length);
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        // Check bounds without summing position + length (which can overflow long for huge positions)
        // and so that toIntExact below sees an in-range value rather than throwing ArithmeticException.
        long blobSize = data.length();
        if (position > blobSize - length) {
            throw new EOFException("Cannot read %s bytes at %s. Blob size is %s".formatted(length, position, blobSize));
        }
        data.getBytes(toIntExact(position), buffer, offset, length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length)
    {
        // The slice was populated from a tail-anchored read, so its bytes ARE the actual file tail.
        // Any tail read of N bytes (N <= slice length) is the last N bytes of the slice.
        checkFromIndexSize(offset, length, buffer.length);
        int readSize = min(length, data.length());
        data.getBytes(data.length() - readSize, buffer, offset, readSize);
        return readSize;
    }

    @Override
    public void close() {}
}
