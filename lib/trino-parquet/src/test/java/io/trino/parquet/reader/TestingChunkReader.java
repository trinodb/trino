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
package io.trino.parquet.reader;

import io.airlift.slice.Slice;
import io.trino.parquet.ChunkReader;
import jakarta.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class TestingChunkReader
        implements ChunkReader
{
    private final long diskOffset;
    @Nullable
    private Slice slice;
    private boolean read;

    public TestingChunkReader(Slice slice)
    {
        this(slice, 0);
    }

    public TestingChunkReader(Slice slice, long diskOffset)
    {
        this.slice = requireNonNull(slice, "slice is null");
        this.diskOffset = diskOffset;
    }

    @Override
    public Slice read()
    {
        read = true;
        return requireNonNull(slice, "slice is null");
    }

    @Override
    public long getDiskOffset()
    {
        return diskOffset;
    }

    @Override
    public void free()
    {
        slice = null;
    }

    public boolean isFreed()
    {
        return slice == null;
    }

    public boolean isRead()
    {
        return read;
    }
}
