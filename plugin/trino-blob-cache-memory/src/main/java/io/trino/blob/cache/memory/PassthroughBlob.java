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

import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobSource;

import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

/**
 * Uncached pass-through Blob returned when the source exceeds the cache's
 * max content length. Reads go straight to the source on every call.
 */
final class PassthroughBlob
        implements Blob
{
    private final BlobSource source;
    private final long length;

    PassthroughBlob(BlobSource source)
            throws IOException
    {
        this.source = requireNonNull(source, "source is null");
        this.length = source.length();
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        source.readFully(position, buffer, offset, length);
    }

    @Override
    public InputStream openStream()
    {
        return new BlobSourceInputStream(source, length);
    }

    @Override
    public void close() {}
}
