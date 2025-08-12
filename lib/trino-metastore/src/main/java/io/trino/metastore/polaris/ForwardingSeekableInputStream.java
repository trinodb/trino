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
package io.trino.metastore.polaris;

import io.trino.filesystem.TrinoInputStream;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

/**
 * Forwarding SeekableInputStream implementation that delegates to Trino's TrinoInputStream.
 */
public class ForwardingSeekableInputStream
        extends SeekableInputStream
{
    private final TrinoInputStream stream;

    public ForwardingSeekableInputStream(TrinoInputStream stream)
    {
        this.stream = requireNonNull(stream, "stream is null");
    }

    @Override
    public long getPos()
            throws IOException
    {
        return stream.getPosition();
    }

    @Override
    public void seek(long pos)
            throws IOException
    {
        stream.seek(pos);
    }

    @Override
    public int read()
            throws IOException
    {
        return stream.read();
    }

    @Override
    public int read(byte[] b)
            throws IOException
    {
        return stream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        return stream.read(b, off, len);
    }

    @Override
    public byte[] readAllBytes()
            throws IOException
    {
        return stream.readAllBytes();
    }

    @Override
    public byte[] readNBytes(int len)
            throws IOException
    {
        return stream.readNBytes(len);
    }

    @Override
    public int readNBytes(byte[] b, int off, int len)
            throws IOException
    {
        return stream.readNBytes(b, off, len);
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        return stream.skip(n);
    }

    @Override
    public void skipNBytes(long n)
            throws IOException
    {
        stream.skipNBytes(n);
    }

    @Override
    public int available()
            throws IOException
    {
        return stream.available();
    }

    @Override
    public void close()
            throws IOException
    {
        stream.close();
    }

    @Override
    public void mark(int readlimit)
    {
        stream.mark(readlimit);
    }

    @Override
    public void reset()
            throws IOException
    {
        stream.reset();
    }

    @Override
    public boolean markSupported()
    {
        return stream.markSupported();
    }

    @Override
    public long transferTo(OutputStream out)
            throws IOException
    {
        return stream.transferTo(out);
    }
}
