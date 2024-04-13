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
package io.trino.filesystem.alluxio;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.memory.MemoryFileSystem;

import java.io.IOException;
import java.time.Instant;
import java.util.Random;

import static java.util.Objects.requireNonNull;

/**
 * Simulates a file system where TrinoInputStream.read(buff, off, len) can return < len bytes.
 */
class IncompleteStreamMemoryFileSystem
        extends MemoryFileSystem
{
    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return new IncompleteStreamInputFile(super.newInputFile(location));
    }

    private static class IncompleteStreamInputFile
            implements TrinoInputFile
    {
        private final TrinoInputFile delegate;

        public IncompleteStreamInputFile(TrinoInputFile delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public TrinoInput newInput()
                throws IOException
        {
            return delegate.newInput();
        }

        @Override
        public TrinoInputStream newStream()
                throws IOException
        {
            return new IncompleteTrinoInputStream(delegate.newStream());
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

    static class IncompleteTrinoInputStream
            extends TrinoInputStream
    {
        private final TrinoInputStream delegate;
        private final Random random;

        public IncompleteTrinoInputStream(TrinoInputStream delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.random = new Random(42);
        }

        @Override
        public int read()
                throws IOException
        {
            return delegate.read();
        }

        @Override
        public int read(byte[] buff, int off, int len)
                throws IOException
        {
            return delegate.read(buff, off, random.nextInt(0, len + 1));
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
    }
}
