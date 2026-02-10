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

import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;

import java.util.Optional;

public class LongArrayBufferAdapter
        extends ArrayBufferAdapter<long[]>
{
    public static final LongArrayBufferAdapter LONG_ARRAY_BUFFER_ADAPTER = new LongArrayBufferAdapter();

    @Override
    public long[] createBuffer(int size)
    {
        return new long[size];
    }

    @Override
    public void copy(long[] source, int sourceOffset, long[] destination, int destinationOffset, int length)
    {
        System.arraycopy(source, sourceOffset, destination, destinationOffset, length);
    }

    @Override
    public Block createBlock(long[] buffer, Optional<boolean[]> valueIsNull)
    {
        return new LongArrayBlock(buffer.length, valueIsNull, buffer);
    }

    @Override
    public long getRetainedBytes(long[] buffer)
    {
        return (long) buffer.length * Long.BYTES;
    }

    @Override
    protected int getLength(long[] buffer)
    {
        return buffer.length;
    }
}
