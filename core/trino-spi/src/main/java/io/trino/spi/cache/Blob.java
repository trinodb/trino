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
import java.io.IOException;

/**
 * Positioned reads over a cache entry's content. Implementations only ever serve exact byte
 * ranges of the content they were populated with.
 */
public interface Blob
        extends Closeable
{
    /**
     * Size of the content this blob serves, which is the extent {@link #read} is valid over.
     */
    long length()
            throws IOException;

    /**
     * Reads exactly {@code length} bytes at {@code position} of the entry's content into
     * {@code buffer} at {@code offset}, or fails without reading anything when the range is
     * not fully within the content. A range outside the content fails with an
     * {@link java.io.EOFException} without a partial read.
     */
    void read(long position, byte[] buffer, int offset, int length)
            throws IOException;

    /**
     * Number of bytes this blob served from cached content so far.
     */
    long cachedSize();

    /**
     * Number of bytes this blob fetched from its backing {@link BlobSource} so far.
     */
    long loadedSize();
}
