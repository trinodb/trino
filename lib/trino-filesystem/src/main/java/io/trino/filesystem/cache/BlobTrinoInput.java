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
package io.trino.filesystem.cache;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoInput;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.cache.Blob;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class BlobTrinoInput
        implements TrinoInput
{
    private final Blob blob;
    private boolean closed;

    BlobTrinoInput(Blob blob)
    {
        this.blob = requireNonNull(blob, "blob is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        blob.read(position, buffer, offset, length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(offset, length, buffer.length);
        long blobLength = blob.length();
        int readSize = toIntExact(min(blobLength, length));
        blob.read(blobLength - readSize, buffer, offset, readSize);
        return readSize;
    }

    @Override
    public Metrics getMetrics()
    {
        return new Metrics(ImmutableMap.of(
                "bytesReadFromCache", new LongCount(blob.cachedSize()),
                "bytesReadExternally", new LongCount(blob.loadedSize())));
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            blob.close();
        }
        catch (Exception e) {
            throw new IOException("Could not close cached blob", e);
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input closed: " + blob);
        }
    }

    @Override
    public String toString()
    {
        return blob.toString();
    }
}
