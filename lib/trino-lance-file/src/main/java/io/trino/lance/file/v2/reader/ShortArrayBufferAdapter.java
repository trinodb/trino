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
import io.trino.spi.block.ShortArrayBlock;

import java.util.Optional;

public class ShortArrayBufferAdapter
        extends ArrayBufferAdapter<short[]>
{
    public static final ShortArrayBufferAdapter SHORT_ARRAY_BUFFER_ADAPTER = new ShortArrayBufferAdapter();

    @Override
    public short[] createBuffer(int size)
    {
        return new short[size];
    }

    @Override
    public void copy(short[] source, int sourceOffset, short[] destination, int destinationOffset, int length)
    {
        System.arraycopy(source, sourceOffset, destination, destinationOffset, length);
    }

    @Override
    public Block createBlock(short[] buffer, Optional<boolean[]> valueIsNull)
    {
        return new ShortArrayBlock(buffer.length, valueIsNull, buffer);
    }

    @Override
    public long getRetainedBytes(short[] buffer)
    {
        return (long) buffer.length * Short.BYTES;
    }

    @Override
    protected int getLength(short[] buffer)
    {
        return buffer.length;
    }
}
