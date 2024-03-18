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
package io.trino.plugin.varada.storage.read;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class NativeJufferInputStream
        extends InputStream
{
    private final ByteBuffer nativeBuffer;
    private int jufferPosition;

    public NativeJufferInputStream(ByteBuffer nativeJuffer, int jufferPosition)
    {
        this.nativeBuffer = nativeJuffer;
        nativeBuffer.position(jufferPosition);
    }

    @Override
    public int read()
    {
        throw new UnsupportedOperationException("read not supported");
    }

    @Override
    public int read(byte[] dst, int start, int length)
    {
        nativeBuffer.get(dst, start, length);
        return length;
    }

    @Override
    public long skip(long n)
    {
        if (n <= 0) {
            return 0;
        }
        jufferPosition = (int) (jufferPosition + n);
        nativeBuffer.position(jufferPosition);
        return n;
    }

    public long readLong()
    {
        long ret = nativeBuffer.getLong(jufferPosition);
        jufferPosition += Long.BYTES;
        return ret;
    }

    public byte readByte()
    {
        byte ret = nativeBuffer.get(jufferPosition);
        jufferPosition++;
        return ret;
    }

    public int readInt()
    {
        int ret = nativeBuffer.getInt(jufferPosition);
        jufferPosition += Integer.BYTES;
        return ret;
    }

    public byte[] readBytes(int start, int length)
    {
        nativeBuffer.position(start);
        byte[] dst = new byte[length];
        nativeBuffer.get(dst, 0, length);
        jufferPosition = start + length;
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
        jufferPosition = pos;
        nativeBuffer.position(pos);
    }
}
