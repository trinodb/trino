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
package io.trino.plugin.iceberg.delete;

import io.airlift.units.Duration;
import io.trino.filesystem.ForwardingTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Delays every read request by a fixed latency, so that a local file system behaves like an object store, where the
 * time of a small read is dominated by the round trip of the request.
 */
final class LatencyInjectingFileSystem
        extends ForwardingTrinoFileSystem
{
    private final Duration latency;

    LatencyInjectingFileSystem(TrinoFileSystem delegate, Duration latency)
    {
        super(delegate);
        this.latency = requireNonNull(latency, "latency is null");
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return new LatencyInjectingInputFile(super.newInputFile(location));
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return new LatencyInjectingInputFile(super.newInputFile(location, length));
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return new LatencyInjectingInputFile(super.newInputFile(location, length, lastModified));
    }

    private void awaitLatency()
            throws InterruptedIOException
    {
        try {
            Thread.sleep(latency.toMillis());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted while waiting for the injected read latency");
        }
    }

    private final class LatencyInjectingInputFile
            implements TrinoInputFile
    {
        private final TrinoInputFile delegate;

        private LatencyInjectingInputFile(TrinoInputFile delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public TrinoInput newInput()
                throws IOException
        {
            return new LatencyInjectingInput(delegate.newInput());
        }

        @Override
        public TrinoInputStream newStream()
                throws IOException
        {
            // opening a stream is a single request, the bytes are then streamed back
            awaitLatency();
            return delegate.newStream();
        }

        @Override
        public long length()
                throws IOException
        {
            return delegate.length();
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            return delegate.lastModified();
        }

        @Override
        public boolean exists()
                throws IOException
        {
            return delegate.exists();
        }

        @Override
        public Location location()
        {
            return delegate.location();
        }
    }

    private final class LatencyInjectingInput
            implements TrinoInput
    {
        private final TrinoInput delegate;

        private LatencyInjectingInput(TrinoInput delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void readFully(long position, byte[] buffer, int bufferOffset, int length)
                throws IOException
        {
            awaitLatency();
            delegate.readFully(position, buffer, bufferOffset, length);
        }

        @Override
        public int readTail(byte[] buffer, int bufferOffset, int maxLength)
                throws IOException
        {
            awaitLatency();
            return delegate.readTail(buffer, bufferOffset, maxLength);
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }
    }
}
