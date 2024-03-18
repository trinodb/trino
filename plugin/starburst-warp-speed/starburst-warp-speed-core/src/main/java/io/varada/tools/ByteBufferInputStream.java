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
package io.varada.tools;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream
        extends InputStream
{
    private final ByteBuffer nativeBuffer;
    private final long jufferReadBytes;
    private int jufferPosition;
    private int mark;

    public ByteBufferInputStream(ByteBuffer nativeJuffer, long jufferReadBytes)
    {
        this.nativeBuffer = nativeJuffer.asReadOnlyBuffer();
        this.jufferReadBytes = jufferReadBytes;
        this.jufferPosition = 0;
    }

    @Override
    public int read()
    {
        if ((jufferPosition < jufferReadBytes)) {
            return nativeBuffer.get(jufferPosition++) & 0xFF;
        }
        return -1; //finish
    }

    @Override
    public int read(byte[] b, int off, int len)
    {
        if (jufferPosition < jufferReadBytes) {
            int actualLen = (int) Math.min(jufferReadBytes - jufferPosition, len);
            nativeBuffer.get(b, off, actualLen);
            jufferPosition += actualLen;
            return actualLen;
        }
        return -1; //finish
    }

    @Override
    public long skip(long n)
    {
        if (n <= 0) {
            return 0;
        }
        int toSkip = Math.min((int) n, available());
        jufferPosition += toSkip;
        return toSkip;
    }

    @Override
    public int available()
    {
        return (int) jufferReadBytes - jufferPosition;
    }

    @Override
    public synchronized void mark(int readlimit)
    {
        mark = jufferPosition;
    }

    @Override
    public synchronized void reset()
    {
        jufferPosition = mark;
    }

    @Override
    public boolean markSupported()
    {
        return true;
    }
}
