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
package io.trino.plugin.hudi;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.HideDeltaLakeTables;
import io.trino.plugin.hive.HiveNodePartitioningProvider;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.metastore.thrift.TranslateHiveViews;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.hudi.cache.HudiCacheKeyProvider;
import io.trino.plugin.hudi.stats.ForHudiTableStatistics;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.security.ConnectorIdentity;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HudiModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(HudiTransactionManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(HudiConfig.class);

        binder.bind(boolean.class).annotatedWith(TranslateHiveViews.class).toInstance(false);
        binder.bind(boolean.class).annotatedWith(HideDeltaLakeTables.class).toInstance(false);

        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(HudiSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HudiTableProperties.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).to(HudiSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(HudiPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);

        binder.bind(HudiMetadataFactory.class).in(Scopes.SINGLETON);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        newOptionalBinder(binder, CacheKeyProvider.class).setBinding().to(HudiCacheKeyProvider.class).in(Scopes.SINGLETON);

        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForHudiTableStatistics.class));
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForHudiSplitManager.class));
        closingBinder(binder).registerExecutor(Key.get(ScheduledExecutorService.class, ForHudiSplitSource.class));
    }

    @Provides
    @Singleton
    @ForHudiTableStatistics
    public ExecutorService createTableStatisticsExecutor(HudiConfig hudiConfig)
    {
        return newScheduledThreadPool(
                hudiConfig.getTableStatisticsExecutorParallelism(),
                daemonThreadsNamed("hudi-table-statistics-executor-%s"));
    }

    @Provides
    @Singleton
    @ForHudiSplitManager
    public ExecutorService createExecutorService()
    {
        return newCachedThreadPool(daemonThreadsNamed("hudi-split-manager-%s"));
    }

    @Provides
    @Singleton
    @ForHudiSplitSource
    public ScheduledExecutorService createSplitLoaderExecutor(HudiConfig hudiConfig)
    {
        return newScheduledThreadPool(
                hudiConfig.getSplitLoaderParallelism(),
                daemonThreadsNamed("hudi-split-loader-%s"));
    }

    @Provides
    @Singleton
    public BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> createHiveMetastoreGetter(HudiTransactionManager transactionManager)
    {
        return (identity, transactionHandle) ->
                transactionManager.get(transactionHandle, identity).getMetastore();
    }
}
