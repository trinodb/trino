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
package io.trino.plugin.deltalake;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.manager.TrinoFileSystemModule;

import static java.util.Objects.requireNonNull;

public class DeltaLakeFileSystemModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;
    private final boolean isCoordinator;
    private final OpenTelemetry openTelemetry;
    private final boolean coordinatorFileCaching;

    public DeltaLakeFileSystemModule(String catalogName, boolean isCoordinator, OpenTelemetry openTelemetry, boolean coordinatorFileCaching)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.isCoordinator = isCoordinator;
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.coordinatorFileCaching = coordinatorFileCaching;
    }

    @Override
    protected void setup(Binder binder)
    {
        install(new TrinoFileSystemModule(catalogName, isCoordinator, openTelemetry, coordinatorFileCaching));
    }

//    @Provides
//    @Singleton
//    static DeltaLakeFileSystemFactory createFileSystemFactory(
//            FileSystemConfig config,
//            Optional<HdfsFileSystemLoader> hdfsFileSystemLoader,
//            Map<String, TrinoFileSystemFactory> factories,
//            Optional<TrinoFileSystemCache> fileSystemCache,
//            Optional<MemoryFileSystemCache> memoryFileSystemCache,
//            Optional<CacheKeyProvider> keyProvider,
//            Tracer tracer)
//    {
//        TrinoFileSystemFactory defaultFileSystemFactory = createDefaultFileSystemFactory(config, hdfsFileSystemLoader, factories, fileSystemCache, memoryFileSystemCache, keyProvider, tracer);
//        return new DefaultDeltaLakeFileSystemFactory(defaultFileSystemFactory);
//    }
}
