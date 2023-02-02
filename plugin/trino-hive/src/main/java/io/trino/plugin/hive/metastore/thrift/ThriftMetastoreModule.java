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
package io.trino.plugin.hive.metastore.thrift;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.OptionalBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.security.UserNameProvider;
import io.trino.plugin.hive.AllowHiveTableRename;
import io.trino.plugin.hive.ForHiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;

import javax.annotation.PreDestroy;

import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.security.UserNameProvider.SIMPLE_USER_NAME_PROVIDER;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ThriftMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        OptionalBinder.newOptionalBinder(binder, ThriftMetastoreClientFactory.class)
                .setDefault().to(DefaultThriftMetastoreClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(TokenAwareMetastoreClientFactory.class).to(StaticTokenAwareMetastoreClientFactory.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticMetastoreConfig.class);
        configBinder(binder).bindConfig(ThriftMetastoreConfig.class);

        binder.bind(ThriftMetastoreFactory.class).to(ThriftHiveMetastoreFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ThriftMetastoreFactory.class)
                .as(generator -> generator.generatedNameOf(ThriftHiveMetastore.class));

        binder.bind(HiveMetastoreFactory.class)
                .annotatedWith(RawHiveMetastoreFactory.class)
                .to(BridgingHiveMetastoreFactory.class)
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(UserNameProvider.class, ForHiveMetastore.class))
                .setDefault()
                .toInstance(SIMPLE_USER_NAME_PROVIDER);

        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(true);

        install(new ThriftMetastoreAuthenticationModule());
    }

    @Provides
    @Singleton
    @ThriftHiveWriteStatisticsExecutor
    public ExecutorService createWriteStatisticsExecutor(ThriftMetastoreConfig hiveConfig)
    {
        return newFixedThreadPool(hiveConfig.getWriteStatisticsThreads(), threadsNamed("hive-thrift-statistics-write-%s"));
    }

    @PreDestroy
    public void shutdownsWriteStatisticExecutor(@ThriftHiveWriteStatisticsExecutor ExecutorService executor)
    {
        executor.shutdownNow();
    }
}
