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
package io.trino.parquet.reader.flat;

import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;

import java.util.Optional;

public class LongColumnAdapter
        implements ColumnAdapter<long[]>
{
    public static final LongColumnAdapter LONG_ADAPTER = new LongColumnAdapter();

    @Override
    public long[] createBuffer(int size)
    {
        return new long[size];
    }

    @Override
    public Block createNonNullBlock(int size, long[] values)
    {
        return new LongArrayBlock(size, Optional.empty(), values);
    }

    @Override
    public Block createNullableBlock(int size, boolean[] nulls, long[] values)
    {
        return new LongArrayBlock(size, Optional.of(nulls), values);
    }

    @Override
    public void copyValue(long[] source, int sourceIndex, long[] destination, int destinationIndex)
    {
        destination[destinationIndex] = source[sourceIndex];
    }
}
