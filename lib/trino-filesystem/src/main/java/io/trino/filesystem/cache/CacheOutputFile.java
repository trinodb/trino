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
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

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
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        return new FilterOutputStream(delegate.create(memoryContext))
        {
            @Override
            public void write(byte[] buffer, int offset, int length)
                    throws IOException
            {
                out.write(buffer, offset, length);
            }

            @Override
            public void close()
                    throws IOException
            {
                try {
                    super.close();
                }
                finally {
                    // Invalidate even when the close fails: a partial file may have been written
                    invalidation.run();
                }
            }
        };
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
}
