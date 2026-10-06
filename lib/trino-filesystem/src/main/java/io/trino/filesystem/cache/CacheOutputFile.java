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
package io.trino.filesystem.cache;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.TrinoOutputStream;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Invalidates the file's cache entries when a write completes, so a concurrent read during
 * the write cannot leave stale content cached.
 */
final class CacheOutputFile
        implements TrinoOutputFile
{
    private final TrinoOutputFile delegate;
    private final Runnable invalidation;

    CacheOutputFile(TrinoOutputFile delegate, Runnable invalidation)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.invalidation = requireNonNull(invalidation, "invalidation is null");
    }

    @Override
    public TrinoOutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        return new CacheOutputStream(delegate.create(memoryContext), invalidation);
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        try {
            delegate.createOrOverwrite(data);
        }
        finally {
            // Invalidate even when the write fails: a partial file may have been written
            invalidation.run();
        }
    }

    @Override
    public void createExclusive(byte[] data)
            throws IOException
    {
        try {
            delegate.createExclusive(data);
        }
        finally {
            invalidation.run();
        }
    }

    @Override
    public Location location()
    {
        return delegate.location();
    }

    private static final class CacheOutputStream
            extends TrinoOutputStream
    {
        private final TrinoOutputStream delegate;
        private final Runnable invalidation;

        private CacheOutputStream(TrinoOutputStream delegate, Runnable invalidation)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.invalidation = requireNonNull(invalidation, "invalidation is null");
        }

        @Override
        public void write(int b)
                throws IOException
        {
            delegate.write(b);
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
            try {
                delegate.close();
            }
            finally {
                // Invalidate even when the close fails: a partial file may have been written
                invalidation.run();
            }
        }

        @Override
        public void abort()
                throws IOException
        {
            try {
                delegate.abort();
            }
            finally {
                // Invalidate on abort: some file systems create the file when the stream is opened
                invalidation.run();
            }
        }
    }
}
