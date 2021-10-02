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
import io.trino.plugin.hive.metastore.cache.SharedHiveMetastoreCache;
import io.trino.plugin.hive.metastore.procedure.FlushHiveMetastoreCacheProcedure;
import io.trino.plugin.hive.metastore.recording.RecordingHiveMetastoreDecoratorModule;
import io.trino.spi.procedure.Procedure;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
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
        binder.bind(SharedHiveMetastoreCache.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HiveMetastore.class)
                .as(generator -> generator.generatedNameOf(CachingHiveMetastore.class));

        newSetBinder(binder, Procedure.class).addBinding().toProvider(FlushHiveMetastoreCacheProcedure.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static HiveMetastore createHiveMetastore(
            @RawHiveMetastore HiveMetastore metastore,
            Set<HiveMetastoreDecorator> decorators,
            SharedHiveMetastoreCache sharedHiveMetastoreCache)
    {
        // wrap the raw metastore with decorators like the RecordingHiveMetastore
        List<HiveMetastoreDecorator> sortedDecorators = decorators.stream()
                .sorted(Comparator.comparing(HiveMetastoreDecorator::getPriority))
                .collect(toImmutableList());
        for (HiveMetastoreDecorator decorator : sortedDecorators) {
            metastore = decorator.decorate(metastore);
        }

        // finally, if the shared metastore cache is enabled wrapper with a global cache
        return sharedHiveMetastoreCache.createSharedHiveMetastoreCache(metastore);
    }

    @Provides
    @Singleton
    public static Optional<CachingHiveMetastore> createHiveMetastore(HiveMetastore metastore)
    {
        if (metastore instanceof CachingHiveMetastore) {
            return Optional.of((CachingHiveMetastore) metastore);
        }
        return Optional.empty();
    }
}
