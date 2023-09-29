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
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class CacheInputFile
        implements TrinoInputFile
{
    private final TrinoInputFile delegate;
    private final TrinoFileSystemCache cache;
    private final CacheKeyProvider keyProvider;

    public CacheInputFile(TrinoInputFile delegate, TrinoFileSystemCache cache, CacheKeyProvider keyProvider)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cache = requireNonNull(cache, "cache is null");
        this.keyProvider = requireNonNull(keyProvider, "keyProvider is null");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        Optional<String> key = keyProvider.getCacheKey(delegate);
        if (key.isPresent()) {
            return cache.cacheInput(delegate, key.orElseThrow());
        }
        return delegate.newInput();
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        Optional<String> key = keyProvider.getCacheKey(delegate);
        if (key.isPresent()) {
            return cache.cacheStream(delegate, key.orElseThrow());
        }
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
