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
package io.trino.sql.parser.utils;

public class ByteBuffer
{
    private byte[] buffer;
    private int capacity;
    private int offset;

    public ByteBuffer()
    {
        this(1024);
    }

    public ByteBuffer(int capacity)
    {
        buffer = new byte[capacity];
        this.capacity = capacity;
    }

    public void append(byte[] end)
    {
        if (null == end) {
            throw new NullPointerException("not support null buffer to put!");
        }

        if (end.length > capacity - offset) {
            realloc(end.length + offset * 2);
        }
        offset += end.length;
        System.arraycopy(end, 0, buffer, offset, end.length);
    }

    public void append(byte[] end, int from, int length)
    {
        if (null == end) {
            throw new NullPointerException("not support null buffer to put!");
        }

        if (from > end.length || length + from > end.length) {
            throw new IndexOutOfBoundsException();
        }

        if (length > capacity - offset) {
            realloc(end.length + offset * 2);
        }
        System.arraycopy(end, from, buffer, offset, length);
        offset += length;
    }

    private void realloc(int newSize)
    {
        byte[] tmp = buffer;
        buffer = new byte[newSize];
        capacity = newSize;
        System.arraycopy(tmp, 0, buffer, 0, offset);
    }

    public int releaseData()
    {
        offset = 0;
        return capacity;
    }

    public int getCapacity()
    {
        return capacity;
    }

    public byte[] getData()
    {
        byte[] data = new byte[offset];
        System.arraycopy(buffer, 0, data, 0, offset);
        return data;
    }

    public int size()
    {
        return offset;
    }
}
