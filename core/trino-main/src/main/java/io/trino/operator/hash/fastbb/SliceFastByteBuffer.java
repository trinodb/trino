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
package io.trino.operator.hash.fastbb;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class SliceFastByteBuffer
        implements FastByteBuffer
{
    private final Slice slice;

    public SliceFastByteBuffer(int capacity)
    {
        slice = Slices.allocate(capacity);
    }

    @Override
    public void copyFrom(FastByteBuffer src, int srcPosition, int destPosition, int length)
    {
        slice.setBytes(destPosition, src.asSlice(), srcPosition, length);
    }

    @Override
    public void putInt(int position, int value)
    {
        slice.setInt(position, value);
    }

    @Override
    public int getInt(int position)
    {
        return slice.getInt(position);
    }

    @Override
    public int capacity()
    {
        return slice.length();
    }

    @Override
    public void putLong(int position, long value)
    {
        slice.setLong(position, value);
    }

    @Override
    public long getLong(int position)
    {
        return slice.getLong(position);
    }

    @Override
    public byte get(int position)
    {
        return slice.getByte(position);
    }

    @Override
    public void put(int position, byte value)
    {
        slice.setByte(position, value);
    }

    @Override
    public short getShort(int position)
    {
        return slice.getShort(position);
    }

    @Override
    public void putShort(int position, short value)
    {
        slice.setShort(position, value);
    }

    @Override
    public Slice asSlice()
    {
        return slice;
    }
}
