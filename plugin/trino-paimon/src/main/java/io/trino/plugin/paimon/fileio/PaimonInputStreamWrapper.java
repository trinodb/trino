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

/**
 * Trino input stream wrapper for paimon.
 */
public class PaimonInputStreamWrapper
        extends SeekableInputStream
{
    private final TrinoInputStream trinoInputStream;

    public PaimonInputStreamWrapper(TrinoInputStream trinoInputStream)
    {
        this.trinoInputStream = requireNonNull(trinoInputStream, "trino input stream is null");
    }

    @Override
    public void seek(long l)
            throws IOException
    {
        trinoInputStream.seek(l);
    }

    @Override
    public long getPos()
            throws IOException
    {
        return trinoInputStream.getPosition();
    }

    @Override
    public int read()
            throws IOException
    {
        return trinoInputStream.read();
    }

    @Override
    public int read(byte[] bytes, int off, int len)
            throws IOException
    {
        return trinoInputStream.read(bytes, off, len);
    }

    @Override
    public void close()
            throws IOException
    {
        trinoInputStream.close();
    }

    @Override
    public byte[] readNBytes(int len)
            throws IOException
    {
        return trinoInputStream.readNBytes(len);
    }

    @Override
    public int readNBytes(byte[] b, int off, int len)
            throws IOException
    {
        return trinoInputStream.readNBytes(b, off, len);
    }

    @Override
    public int read(byte[] b)
            throws IOException
    {
        return trinoInputStream.read(b);
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        return trinoInputStream.skip(n);
    }

    @Override
    public void skipNBytes(long n)
            throws IOException
    {
        trinoInputStream.skipNBytes(n);
    }

    @Override
    public byte[] readAllBytes()
            throws IOException
    {
        return trinoInputStream.readAllBytes();
    }

    @Override
    public int available()
            throws IOException
    {
        return trinoInputStream.available();
    }

    @Override
    public void mark(int readlimit)
    {
        trinoInputStream.mark(readlimit);
    }

    @Override
    public void reset()
            throws IOException
    {
        trinoInputStream.reset();
    }

    @Override
    public boolean markSupported()
    {
        return trinoInputStream.markSupported();
    }

    @Override
    public long transferTo(OutputStream out)
            throws IOException
    {
        return trinoInputStream.transferTo(out);
    }
}
