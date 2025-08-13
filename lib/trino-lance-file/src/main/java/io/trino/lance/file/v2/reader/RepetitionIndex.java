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
package io.trino.lance.file.v2.reader;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class RepetitionIndex
{
    private RepetitionIndex() {}

    public static List<RepetitionIndex.RepIndexBlock> from(Slice slice, int depth)
    {
        checkArgument(depth > 0, "depth must be positive");
        boolean hasPreamble = false;
        long offset = 0;
        ImmutableList.Builder<RepIndexBlock> builder = ImmutableList.builder();
        int stride = (depth + 1) * Long.BYTES;
        for (int i = 0; i < slice.length() / stride; i++) {
            long endCount = slice.getLong(i * stride);
            long partialCount = slice.getLong(i * stride + Long.BYTES);
            boolean hasTrailer = partialCount > 0;
            long startCount = endCount + (hasTrailer ? 1 : 0) - (hasPreamble ? 1 : 0);
            builder.add(new RepIndexBlock(offset, startCount, hasPreamble, hasTrailer));
            hasPreamble = hasTrailer;
            offset += startCount;
        }
        return builder.build();
    }

    public static List<RepIndexBlock> defaultIndex(List<ChunkMetadata> chunks)
    {
        long offset = 0;
        ImmutableList.Builder<RepIndexBlock> builder = ImmutableList.builder();
        for (ChunkMetadata chunk : chunks) {
            long count = chunk.numValues();
            builder.add(new RepIndexBlock(offset, count, false, false));
            offset += count;
        }
        return builder.build();
    }

    // startCount is number of rows starts in this block(i.e., include trailer but not preamble)
    public record RepIndexBlock(long firstRow, long startCount, boolean hasPreamble, boolean hasTrailer) {}
}
