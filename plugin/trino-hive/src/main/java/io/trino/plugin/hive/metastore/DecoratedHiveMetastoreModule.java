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
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.cache.ImpersonationCachingConfig;
import io.trino.plugin.hive.metastore.cache.SharedHiveMetastoreCache;
import io.trino.plugin.hive.metastore.cache.SharedHiveMetastoreCache.CachingHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.procedure.FlushHiveMetastoreCacheProcedure;
import io.trino.plugin.hive.metastore.recording.RecordingHiveMetastoreDecoratorModule;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DecoratedHiveMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newSetBinder(binder, HiveMetastoreDecorator.class);
        install(new RecordingHiveMetastoreDecoratorModule());

        configBinder(binder).bindConfig(CachingHiveMetastoreConfig.class);
        // TODO this should only be bound when impersonation is actually enabled
        configBinder(binder).bindConfig(ImpersonationCachingConfig.class);
        binder.bind(SharedHiveMetastoreCache.class).in(Scopes.SINGLETON);
        // export under the old name, for backwards compatibility
        newExporter(binder).export(HiveMetastoreFactory.class)
                .as(generator -> generator.generatedNameOf(CachingHiveMetastore.class));

        newSetBinder(binder, Procedure.class).addBinding().toProvider(FlushHiveMetastoreCacheProcedure.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static HiveMetastoreFactory createHiveMetastore(
            @RawHiveMetastoreFactory HiveMetastoreFactory metastoreFactory,
            Set<HiveMetastoreDecorator> decorators,
            SharedHiveMetastoreCache sharedHiveMetastoreCache)
    {
        // wrap the raw metastore with decorators like the RecordingHiveMetastore
        metastoreFactory = new DecoratingHiveMetastoreFactory(metastoreFactory, decorators);

        // cross TX metastore cache is enabled wrapper with caching metastore
        return sharedHiveMetastoreCache.createCachingHiveMetastoreFactory(metastoreFactory);
    }

    private static class DecoratingHiveMetastoreFactory
            implements HiveMetastoreFactory
    {
        private final HiveMetastoreFactory delegate;
        private final List<HiveMetastoreDecorator> sortedDecorators;

        public DecoratingHiveMetastoreFactory(HiveMetastoreFactory delegate, Set<HiveMetastoreDecorator> decorators)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");

            this.sortedDecorators = requireNonNull(decorators, "decorators is null").stream()
                    .sorted(Comparator.comparing(HiveMetastoreDecorator::getPriority))
                    .collect(toImmutableList());
        }

        @Override
        public boolean isImpersonationEnabled()
        {
            return delegate.isImpersonationEnabled();
        }

        @Override
        public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
        {
            HiveMetastore metastore = delegate.createMetastore(identity);
            for (HiveMetastoreDecorator decorator : sortedDecorators) {
                metastore = decorator.decorate(metastore);
            }
            return metastore;
        }
    }

    @Provides
    @Singleton
    public static Optional<CachingHiveMetastore> createHiveMetastore(HiveMetastoreFactory metastoreFactory)
    {
        if (metastoreFactory instanceof CachingHiveMetastoreFactory) {
            return Optional.of((((CachingHiveMetastoreFactory) metastoreFactory).getMetastore()));
        }
        return Optional.empty();
    }
}
