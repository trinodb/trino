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

import java.io.IOException;
import java.io.OutputStream;
import java.lang.ref.Cleaner;

import static java.util.Objects.requireNonNull;

public class TrackingOutputStream
        extends OutputStream
{
    private final OutputStream delegate;
    private final TrackingState state;

    public TrackingOutputStream(OutputStream delegate, Location location, Cleaner cleaner)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.state = new TrackingState(delegate, location);
        cleaner.register(this, state);
    }

    @Override
    public void write(byte[] buffer)
            throws IOException
    {
        delegate.write(buffer);
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        delegate.write(buffer, offset, length);
    }

    @Override
    public void flush()
            throws IOException
    {
        delegate.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        state.markClosed();
        delegate.close();
    }

    @Override
    public void write(int b)
            throws IOException
    {
        delegate.write(b);
    }
}
