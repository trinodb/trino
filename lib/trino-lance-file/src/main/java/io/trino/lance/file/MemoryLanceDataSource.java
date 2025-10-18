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

import java.io.IOException;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MemoryLanceDataSource
        implements LanceDataSource
{
    private final LanceDataSourceId id;
    private final Slice data;
    private long readBytes;

    public MemoryLanceDataSource(LanceDataSourceId id, Slice data)
    {
        this.id = requireNonNull(id, "id is null");
        this.data = requireNonNull(data, "data is null");
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
        return 0;
    }

    @Override
    public long getEstimatedSize()
    {
        return data.length();
    }

    @Override
    public long getRetainedSize()
    {
        return data.getRetainedSize();
    }

    @Override
    public Slice readTail(int length)
            throws IOException
    {
        int readSize = min(data.length(), length);
        return readFully(data.length() - readSize, readSize);
    }

    @Override
    public Slice readFully(long position, int length)
            throws IOException
    {
        readBytes += length;
        return data.slice(toIntExact(position), length);
    }

    @Override
    public final String toString()
    {
        return id.toString();
    }
}
