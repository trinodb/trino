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
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;

import java.util.Optional;

public class IntArrayBufferAdapter
        extends ArrayBufferAdapter<int[]>
{
    public static final IntArrayBufferAdapter INT_ARRAY_BUFFER_ADAPTER = new IntArrayBufferAdapter();

    @Override
    public int[] createBuffer(int size)
    {
        return new int[size];
    }

    @Override
    public void copy(int[] source, int sourceOffset, int[] destination, int destinationOffset, int length)
    {
        System.arraycopy(source, sourceOffset, destination, destinationOffset, length);
    }

    @Override
    public Block createBlock(int[] buffer, Optional<boolean[]> valueIsNull)
    {
        return new IntArrayBlock(buffer.length, valueIsNull, buffer);
    }

    @Override
    public long getRetainedBytes(int[] buffer)
    {
        return (long) buffer.length * Integer.BYTES;
    }

    @Override
    protected int getLength(int[] buffer)
    {
        return buffer.length;
    }

    @Override
    public Block createDictionaryBlock(int[] ids, Block dictionary, Optional<boolean[]> valueIsNull)
    {
        if (valueIsNull.isPresent()) {
            boolean[] isNull = valueIsNull.get();
            for (int i = 0; i < ids.length; i++) {
                if (isNull[i]) {
                    ids[i] = dictionary.getPositionCount() - 1;
                }
            }
        }
        return DictionaryBlock.create(ids.length, dictionary, ids);
    }
}
