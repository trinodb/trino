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
package io.trino.plugin.paimon.fileio;

import org.apache.paimon.fs.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Wrapper of {@link OutputStream}.
 */
public class PositionOutputStreamWrapper
        extends PositionOutputStream
{
    private final OutputStream outputStream;

    private long position;

    public PositionOutputStreamWrapper(OutputStream outputStream)
    {
        this(outputStream, 0);
    }

    public PositionOutputStreamWrapper(OutputStream outputStream, long startPosition)
    {
        this.outputStream = outputStream;
        this.position = startPosition;
    }

    @Override
    public long getPos()
    {
        return position;
    }

    @Override
    public void write(int b)
            throws IOException
    {
        position++;
        outputStream.write(b);
    }

    @Override
    public void write(byte[] bytes)
            throws IOException
    {
        position += bytes.length;
        outputStream.write(bytes);
    }

    @Override
    public void write(byte[] bytes, int off, int len)
            throws IOException
    {
        position += len;
        outputStream.write(bytes, off, len);
    }

    @Override
    public void flush()
            throws IOException
    {
        outputStream.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        outputStream.close();
    }
}
