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

import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;

import java.io.IOException;
import java.lang.ref.Cleaner;

import static java.util.Objects.requireNonNull;

public class TrackingInput
        implements TrinoInput
{
    private final TrinoInput delegate;
    private final TrackingState state;

    public TrackingInput(TrinoInput delegate, Location location, Cleaner cleaner)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.state = new TrackingState(delegate, location);
        cleaner.register(this, state);
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        delegate.readFully(position, buffer, bufferOffset, bufferLength);
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        return delegate.readTail(buffer, bufferOffset, bufferLength);
    }

    @Override
    public Slice readFully(long position, int length)
            throws IOException
    {
        return delegate.readFully(position, length);
    }

    @Override
    public Slice readTail(int length)
            throws IOException
    {
        return delegate.readTail(length);
    }

    @Override
    public void close()
            throws IOException
    {
        state.markClosed();
        delegate.close();
    }
}
