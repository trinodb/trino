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
package io.trino.filesystem.cache.local;

import com.google.inject.Binder;
import com.google.inject.Provider;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.cache.CacheSplitAffinityProvider;
import io.trino.filesystem.cache.SplitAffinityProvider;
import io.trino.filesystem.cache.TrinoFileSystemCache;
import io.trino.spi.catalog.CatalogName;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class NativeFileSystemCacheModule
        extends AbstractConfigurationAwareModule
{
    private final boolean isCoordinator;

    public NativeFileSystemCacheModule(boolean isCoordinator)
    {
        this.isCoordinator = isCoordinator;
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(NativeFileSystemCacheConfig.class);
        binder.bind(NativeFileSystemCacheStats.class).in(SINGLETON);
        Provider<CatalogName> catalogName = binder.getProvider(CatalogName.class);
        newExporter(binder).export(NativeFileSystemCacheStats.class)
                .as(generator -> generator.generatedNameOf(NativeFileSystemCacheStats.class, catalogName.get().toString()));

        if (isCoordinator) {
            newOptionalBinder(binder, SplitAffinityProvider.class).setBinding().to(CacheSplitAffinityProvider.class).in(SINGLETON);
        }
        binder.bind(TrinoFileSystemCache.class).to(NativeFileSystemCache.class).in(SINGLETON);
    }
}
