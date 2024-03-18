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
package io.trino.plugin.varada.juffer;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream
        extends InputStream
{
    private final ByteBuffer byteBuffer;

    public ByteBufferInputStream(ByteBuffer byteBuffer, int jufferPosition)
    {
        this.byteBuffer = byteBuffer;
        byteBuffer.position(jufferPosition);
    }

    public ByteBufferInputStream(ByteBuffer byteBuffer)
    {
        this(byteBuffer, 0);
    }

    @Override
    public int read()
    {
        throw new UnsupportedOperationException("read not supported");
    }

    @Override
    public int read(byte[] dst, int start, int length)
    {
        byteBuffer.get(dst, start, length);
        return length;
    }

    @Override
    public long skip(long n)
    {
        if (n <= 0) {
            return 0;
        }
        byteBuffer.position(byteBuffer.position() + (int) n);
        return n;
    }

    public long[] readLongs(int position, int numLongs)
    {
        long[] dst = new long[numLongs];
        byteBuffer.position(position);
        for (int i = 0; i < numLongs; i++) {
            dst[i] = byteBuffer.getLong();
        }
        return dst;
    }

    public byte readByte()
    {
        return byteBuffer.get();
    }

    public long readLong()
    {
        return byteBuffer.getLong();
    }

    public byte[] readBytes(int position, int numBytes)
    {
        byte[] dst = new byte[numBytes];
        byteBuffer.position(position);
        byteBuffer.get(dst);
        return dst;
    }

    public int readInt()
    {
        return byteBuffer.getInt();
    }

    public int[] readInts(int position, int numInts)
    {
        int[] dst = new int[numInts];
        byteBuffer.position(position);
        for (int i = 0; i < numInts; i++) {
            dst[i] = byteBuffer.getInt();
        }
        return dst;
    }

    @Override
    public int available()
    {
        throw new UnsupportedOperationException("available not supported");
    }

    @Override
    public boolean markSupported()
    {
        return true;
    }

    @Override
    public synchronized void reset()
    {
        position(0);
    }

    public void position(int pos)
    {
        byteBuffer.position(pos);
    }

    public int position()
    {
        return byteBuffer.position();
    }
}
