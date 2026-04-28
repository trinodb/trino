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

import java.io.IOException;

/**
 * Lazy source of bytes backing a cached blob. Used by {@link BlobCache} to
 * populate a cache entry on miss.
 */
public interface BlobSource
{
    long length()
            throws IOException;

    void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException;

    /**
     * Read exactly {@code length} bytes anchored at the actual end of the underlying object.
     * Implementations must address this read using the source's true byte range
     * (e.g. an HTTP suffix-range request), independent of any value returned by {@link #length()}
     * — which may be a caller-supplied hint that does not match the storage object.
     */
    void readTail(byte[] buffer, int offset, int length)
            throws IOException;
}
