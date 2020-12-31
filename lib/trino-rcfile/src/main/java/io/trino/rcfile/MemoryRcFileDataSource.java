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
package io.prestosql.rcfile;

import io.airlift.slice.Slice;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MemoryRcFileDataSource
        implements RcFileDataSource
{
    private final RcFileDataSourceId id;
    private final Slice data;
    private long readBytes;

    public MemoryRcFileDataSource(RcFileDataSourceId id, Slice data)
    {
        this.id = requireNonNull(id, "id is null");
        this.data = requireNonNull(data, "data is null");
    }

    @Override
    public RcFileDataSourceId getId()
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
    public long getSize()
    {
        return data.length();
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        data.getBytes(toIntExact(position), buffer, bufferOffset, bufferLength);
        readBytes += bufferLength;
    }

    @Override
    public void close() {}
}
