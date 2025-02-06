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
package io.trino.plugin.hive.metastore;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.metastore.cache.CachingHiveMetastoreConfig;
import io.trino.metastore.cache.ImpersonationCachingConfig;
import io.trino.metastore.cache.SharedHiveMetastoreCache;
import io.trino.metastore.cache.SharedHiveMetastoreCache.CachingHiveMetastoreFactory;

import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CachingHiveMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(CachingHiveMetastoreConfig.class);
        // TODO this should only be bound when impersonation is actually enabled
        configBinder(binder).bindConfig(ImpersonationCachingConfig.class);
        binder.bind(SharedHiveMetastoreCache.class).in(Scopes.SINGLETON);
        // export under the old name, for backwards compatibility
        newExporter(binder).export(HiveMetastoreFactory.class)
                .as(generator -> generator.generatedNameOf(CachingHiveMetastore.class));
    }

    @Provides
    @Singleton
    public static HiveMetastoreFactory createHiveMetastore(
            @RawHiveMetastoreFactory HiveMetastoreFactory metastoreFactory,
            SharedHiveMetastoreCache sharedHiveMetastoreCache)
    {
        // cross TX metastore cache is enabled wrapper with caching metastore
        return sharedHiveMetastoreCache.createCachingHiveMetastoreFactory(metastoreFactory);
    }

    @Provides
    @Singleton
    public static Optional<CachingHiveMetastore> createHiveMetastore(HiveMetastoreFactory metastoreFactory)
    {
        if (metastoreFactory instanceof CachingHiveMetastoreFactory) {
            return Optional.of(((CachingHiveMetastoreFactory) metastoreFactory).getMetastore());
        }
        return Optional.empty();
    }
}
