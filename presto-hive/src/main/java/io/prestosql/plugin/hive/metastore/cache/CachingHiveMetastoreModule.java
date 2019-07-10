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
package io.prestosql.plugin.hive.metastore.cache;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.concurrent.BoundedExecutor;
import io.prestosql.plugin.hive.HiveCatalogName;
import io.prestosql.plugin.hive.metastore.HiveMetastore;

import javax.inject.Singleton;

import java.util.concurrent.Executor;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CachingHiveMetastoreModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(CachingHiveMetastoreConfig.class);
        binder.bind(HiveMetastore.class).to(CachingHiveMetastore.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HiveMetastore.class)
                .as(generator -> generator.generatedNameOf(CachingHiveMetastore.class));
    }

    @Provides
    @Singleton
    @ForCachingHiveMetastore
    public Executor createCachingHiveMetastoreExecutor(HiveCatalogName catalogName, CachingHiveMetastoreConfig hiveConfig)
    {
        return new BoundedExecutor(
                newCachedThreadPool(daemonThreadsNamed("hive-metastore-" + catalogName + "-%s")),
                hiveConfig.getMaxMetastoreRefreshThreads());
    }
}
