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
package io.trino.filesystem.tracking;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.ref.Cleaner;

import static java.util.Objects.requireNonNull;

public class TrackingInputStream
        extends TrinoInputStream
{
    private final TrinoInputStream delegate;
    private final TrackingState state;

    public TrackingInputStream(TrinoInputStream delegate, Location location, Cleaner cleaner)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.state = new TrackingState(delegate, location);
        cleaner.register(this, state);
    }

    @Override
    public long getPosition()
            throws IOException
    {
        return delegate.getPosition();
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        delegate.seek(position);
    }

    @Override
    public int read()
            throws IOException
    {
        return delegate.read();
    }

    @Override
    public int read(byte[] buffer)
            throws IOException
    {
        return delegate.read(buffer);
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
            throws IOException
    {
        return delegate.read(buffer, offset, length);
    }

    @Override
    public byte[] readAllBytes()
            throws IOException
    {
        return delegate.readAllBytes();
    }

    @Override
    public byte[] readNBytes(int length)
            throws IOException
    {
        return delegate.readNBytes(length);
    }

    @Override
    public int readNBytes(byte[] buffer, int offset, int length)
            throws IOException
    {
        return delegate.readNBytes(buffer, offset, length);
    }

    @Override
    public long skip(long bytes)
            throws IOException
    {
        return delegate.skip(bytes);
    }

    @Override
    public void skipNBytes(long bytes)
            throws IOException
    {
        delegate.skipNBytes(bytes);
    }

    @Override
    public int available()
            throws IOException
    {
        return delegate.available();
    }

    @Override
    public void close()
            throws IOException
    {
        state.markClosed();
        delegate.close();
    }

    @Override
    public void mark(int readlimit)
    {
        delegate.mark(readlimit);
    }

    @Override
    public void reset()
            throws IOException
    {
        delegate.reset();
    }

    @Override
    public boolean markSupported()
    {
        return delegate.markSupported();
    }

    @Override
    public long transferTo(OutputStream out)
            throws IOException
    {
        return delegate.transferTo(out);
    }
}
