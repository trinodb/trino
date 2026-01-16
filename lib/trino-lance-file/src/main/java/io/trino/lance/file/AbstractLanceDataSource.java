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
package io.trino.lance.file;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public abstract class AbstractLanceDataSource
        implements LanceDataSource
{
    private final LanceDataSourceId id;
    private final long estimatedSize;
    private long readTimeNanos;
    private long readBytes;

    public AbstractLanceDataSource(LanceDataSourceId id, long estimatedSize)
    {
        this.id = requireNonNull(id, "id is null");
        this.estimatedSize = estimatedSize;
    }

    @Override
    public LanceDataSourceId getId()
    {
        return id;
    }

    @Override
    public long getReadBytes()
    {
        return readBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getEstimatedSize()
    {
        return estimatedSize;
    }

    @Override
    public Slice readTail(int length)
            throws IOException
    {
        long start = System.nanoTime();

        Slice tailSlice = readTailInternal(length);

        readTimeNanos += System.nanoTime() - start;
        readBytes += tailSlice.length();

        return tailSlice;
    }

    protected Slice readTailInternal(int length)
            throws IOException
    {
        int readSize = toIntExact(min(estimatedSize, length));
        return readFully(estimatedSize - readSize, readSize);
    }

    @Override
    public final Slice readFully(long position, int length)
            throws IOException
    {
        byte[] buffer = new byte[length];
        readFully(position, buffer, 0, length);
        return Slices.wrappedBuffer(buffer);
    }

    protected abstract void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException;

    private void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long start = System.nanoTime();

        readInternal(position, buffer, bufferOffset, bufferLength);

        readTimeNanos += System.nanoTime() - start;
        readBytes += bufferLength;
    }

    @Override
    public final String toString()
    {
        return id.toString();
    }
}
