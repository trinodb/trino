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

/**
 * Like byte array but with fast read/write for other primitive types like int, long, short.
 */
public interface FastByteBuffer
{
    static FastByteBuffer allocate(int capacity)
    {
        return new VarHandleFastByteBuffer(capacity);
    }

    void copyFrom(FastByteBuffer src, int srcPosition, int destPosition, int length);

    void putInt(int position, int value);

    int getInt(int position);

    int capacity();

    void putLong(int position, long value);

    byte get(int position);

    void put(int position, byte value);

    long getLong(int position);

    default void putByteUnsigned(int position, int value)
    {
        put(position, (byte) value);
    }

    default int getByteUnsigned(int position)
    {
        return Byte.toUnsignedInt(get(position));
    }

    short getShort(int position);

    void putShort(int position, short value);

    Slice asSlice();

    default String toString(int position, int length)
    {
        int iMax = position + length - 1;
        if (iMax == -1) {
            return "[]";
        }

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = position; ; i++) {
            b.append(get(i));
            if (i == iMax) {
                return b.append(']').toString();
            }
            b.append(", ");
        }
    }
}
