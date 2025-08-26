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
package io.trino.filesystem.manager;

import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.CacheFileSystemFactory;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.filesystem.cache.TrinoFileSystemCache;
import io.trino.filesystem.memory.MemoryFileSystemCache;
import io.trino.filesystem.switching.SwitchingFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.filesystem.tracking.TrackingFileSystemFactory;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public final class FileSystemUtils
{
    private FileSystemUtils() {}

    public static TrinoFileSystemFactory createDefaultFileSystemFactory(
            FileSystemConfig config,
            Optional<HdfsFileSystemLoader> hdfsFileSystemLoader,
            Map<String, TrinoFileSystemFactory> factories,
            Optional<TrinoFileSystemCache> fileSystemCache,
            Optional<MemoryFileSystemCache> memoryFileSystemCache,
            Optional<CacheKeyProvider> keyProvider,
            Tracer tracer)
    {
        Optional<TrinoFileSystemFactory> hdfsFactory = hdfsFileSystemLoader.map(HdfsFileSystemLoader::create);

        Function<Location, TrinoFileSystemFactory> loader = location -> location.scheme()
                .map(factories::get)
                .or(() -> hdfsFactory)
                .orElseThrow(() -> new IllegalArgumentException("No factory for location: " + location));

        TrinoFileSystemFactory delegate = new SwitchingFileSystemFactory(loader);
        delegate = new TracingFileSystemFactory(tracer, delegate);

        if (config.isTrackingEnabled()) {
            delegate = new TrackingFileSystemFactory(delegate);
        }

        if (fileSystemCache.isPresent()) {
            return new CacheFileSystemFactory(tracer, delegate, fileSystemCache.orElseThrow(), keyProvider.orElseThrow());
        }
        // use MemoryFileSystemCache only when no other TrinoFileSystemCache is configured
        if (memoryFileSystemCache.isPresent()) {
            return new CacheFileSystemFactory(tracer, delegate, memoryFileSystemCache.orElseThrow(), keyProvider.orElseThrow());
        }
        return delegate;
    }
}
