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

import io.trino.filesystem.TrinoInputStream;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

public class PaimonInputStreamWrapper
        extends SeekableInputStream
{
    private final TrinoInputStream inputStream;

    public PaimonInputStreamWrapper(TrinoInputStream inputStream)
    {
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
    }

    @Override
    public void seek(long l)
            throws IOException
    {
        inputStream.seek(l);
    }

    @Override
    public long getPos()
            throws IOException
    {
        return inputStream.getPosition();
    }

    @Override
    public int read()
            throws IOException
    {
        return inputStream.read();
    }

    @Override
    public int read(byte[] bytes, int off, int len)
            throws IOException
    {
        return inputStream.read(bytes, off, len);
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }

    @Override
    public byte[] readNBytes(int len)
            throws IOException
    {
        return inputStream.readNBytes(len);
    }

    @Override
    public int readNBytes(byte[] b, int off, int len)
            throws IOException
    {
        return inputStream.readNBytes(b, off, len);
    }

    @Override
    public int read(byte[] b)
            throws IOException
    {
        return inputStream.read(b);
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        return inputStream.skip(n);
    }

    @Override
    public void skipNBytes(long n)
            throws IOException
    {
        inputStream.skipNBytes(n);
    }

    @Override
    public byte[] readAllBytes()
            throws IOException
    {
        return inputStream.readAllBytes();
    }

    @Override
    public int available()
            throws IOException
    {
        return inputStream.available();
    }

    @Override
    public void mark(int readlimit)
    {
        inputStream.mark(readlimit);
    }

    @Override
    public void reset()
            throws IOException
    {
        inputStream.reset();
    }

    @Override
    public boolean markSupported()
    {
        return inputStream.markSupported();
    }

    @Override
    public long transferTo(OutputStream out)
            throws IOException
    {
        return inputStream.transferTo(out);
    }
}
