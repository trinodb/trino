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
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public class NoopBlob
        implements Blob
{
    private final BlobSource source;
    private final AtomicLong loadedSize = new AtomicLong();

    public NoopBlob(BlobSource source)
    {
        this.source = requireNonNull(source, "source is null");
    }

    @Override
    public long length()
            throws IOException
    {
        return source.length();
    }

    @Override
    public void read(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        source.readFully(position, buffer, offset, length);
        loadedSize.addAndGet(length);
    }

    @Override
    public long cachedSize()
    {
        return 0;
    }

    @Override
    public long loadedSize()
    {
        return loadedSize.get();
    }

    @Override
    public void close()
            throws IOException
    {
        source.close();
    }

    @Override
    public String toString()
    {
        return source.toString();
    }
}
