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

import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemCache;
import io.trino.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

public final class CacheFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final Tracer tracer;
    private final TrinoFileSystemFactory delegate;
    private final TrinoFileSystemCache cache;
    private final CacheKeyProvider keyProvider;

    public CacheFileSystemFactory(Tracer tracer, TrinoFileSystemFactory delegate, TrinoFileSystemCache cache, CacheKeyProvider keyProvider)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cache = requireNonNull(cache, "cache is null");
        this.keyProvider = requireNonNull(keyProvider, "keyProvider is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new CacheFileSystem(delegate.create(identity), new TracingFileSystemCache(tracer, cache), keyProvider);
    }
}
