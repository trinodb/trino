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
package io.trino.orc;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.orc.stream.MemoryOrcDataReader;
import io.trino.orc.stream.OrcDataReader;

import java.util.Map;
import java.util.Map.Entry;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MemoryOrcDataSource
        implements OrcDataSource
{
    private final OrcDataSourceId id;
    private final Slice data;
    private long readBytes;

    public MemoryOrcDataSource(OrcDataSourceId id, Slice data)
    {
        this.id = requireNonNull(id, "id is null");
        this.data = requireNonNull(data, "data is null");
    }

    @Override
    public OrcDataSourceId getId()
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
    public final long getEstimatedSize()
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
    {
        int readSize = min(data.length(), length);
        return readFully(data.length() - readSize, readSize);
    }

    @Override
    public final Slice readFully(long position, int length)
    {
        readBytes += length;
        return data.slice(toIntExact(position), length);
    }

    @Override
    public final <K> Map<K, OrcDataReader> readFully(Map<K, DiskRange> diskRanges)
    {
        requireNonNull(diskRanges, "diskRanges is null");

        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<K, OrcDataReader> slices = ImmutableMap.builder();
        for (Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            DiskRange diskRange = entry.getValue();
            Slice slice = readFully(diskRange.getOffset(), diskRange.getLength());
            // retained memory is reported by this data source, so it should not be declared in the reader
            slices.put(entry.getKey(), new MemoryOrcDataReader(id, slice, 0));
        }

        return slices.buildOrThrow();
    }

    @Override
    public final String toString()
    {
        return id.toString();
    }
}
