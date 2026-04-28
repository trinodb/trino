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
package io.trino.spi.cache;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;

import static java.util.Objects.checkFromIndexSize;

public interface Blob
        extends Closeable
{
    long length()
            throws IOException;

    void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException;

    /**
     * Read exactly {@code length} bytes anchored at the end of the blob.
     * <p>
     * The default implementation derives the position from {@link #length()}, which is correct
     * when the blob's length matches the underlying storage object. Implementations whose
     * {@code length()} may diverge from the storage object (for example, blobs constructed from
     * a caller-supplied length hint such as Iceberg's {@code use_file_size_from_metadata})
     * should override this to read from the source's true byte range.
     */
    default void readTail(byte[] buffer, int offset, int length)
            throws IOException
    {
        checkFromIndexSize(offset, length, buffer.length);
        long size = length();
        if (length > size) {
            throw new EOFException("Cannot read tail of %s bytes. Blob size is %s".formatted(length, size));
        }
        readFully(size - length, buffer, offset, length);
    }
}
