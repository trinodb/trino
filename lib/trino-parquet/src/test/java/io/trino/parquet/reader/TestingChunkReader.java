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
    @Nullable
    private Slice slice;

    public TestingChunkReader(Slice slice)
    {
        this.slice = requireNonNull(slice, "slice is null");
    }

    @Override
    public Slice read()
    {
        return requireNonNull(slice, "slice is null");
    }

    @Override
    public long getDiskOffset()
    {
        return 0;
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
}
