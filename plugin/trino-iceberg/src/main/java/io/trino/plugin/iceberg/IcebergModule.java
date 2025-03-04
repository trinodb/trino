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
package io.trino.plugin.iceberg;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProviderFactory;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.SortingFileWriterConfig;
import io.trino.plugin.hive.metastore.thrift.TranslateHiveViews;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.cache.IcebergCacheKeyProvider;
import io.trino.plugin.iceberg.catalog.rest.DefaultIcebergFileSystemFactory;
import io.trino.plugin.iceberg.functions.IcebergFunctionProvider;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionProcessorProviderFactory;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionProvider;
import io.trino.plugin.iceberg.procedure.AddFilesTableFromTableProcedure;
import io.trino.plugin.iceberg.procedure.AddFilesTableProcedure;
import io.trino.plugin.iceberg.procedure.DropExtendedStatsTableProcedure;
import io.trino.plugin.iceberg.procedure.ExpireSnapshotsTableProcedure;
import io.trino.plugin.iceberg.procedure.OptimizeManifestsTableProcedure;
import io.trino.plugin.iceberg.procedure.OptimizeTableProcedure;
import io.trino.plugin.iceberg.procedure.RegisterTableProcedure;
import io.trino.plugin.iceberg.procedure.RemoveOrphanFilesTableProcedure;
import io.trino.plugin.iceberg.procedure.RollbackToSnapshotProcedure;
import io.trino.plugin.iceberg.procedure.RollbackToSnapshotTableProcedure;
import io.trino.plugin.iceberg.procedure.UnregisterTableProcedure;
import io.trino.plugin.iceberg.system.IcebergTablesSystemTable;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;

import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(IcebergTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, TranslateHiveViews.class)).toInstance(false);
        configBinder(binder).bindConfig(IcebergConfig.class);
        configBinder(binder).bindConfig(SortingFileWriterConfig.class, "iceberg");

        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(IcebergSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(IcebergSchemaProperties.class).in(Scopes.SINGLETON);
        binder.bind(IcebergTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(IcebergMaterializedViewProperties.class).in(Scopes.SINGLETON);
        binder.bind(IcebergAnalyzeProperties.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).annotatedWith(ForClassLoaderSafe.class).to(IcebergSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(ClassLoaderSafeConnectorSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProviderFactory.class).annotatedWith(ForClassLoaderSafe.class).to(IcebergPageSourceProviderFactory.class).in(Scopes.SINGLETON);
        binder.bind(IcebergPageSourceProviderFactory.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProviderFactory.class).to(ClassLoaderSafeConnectorPageSourceProviderFactory.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).annotatedWith(ForClassLoaderSafe.class).to(IcebergPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(ClassLoaderSafeConnectorPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).annotatedWith(ForClassLoaderSafe.class).to(IcebergNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(ClassLoaderSafeNodePartitioningProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(OrcReaderConfig.class);
        configBinder(binder).bindConfig(OrcWriterConfig.class);

        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);

        binder.bind(TableStatisticsWriter.class).in(Scopes.SINGLETON);
        binder.bind(IcebergMetadataFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class));

        jsonCodecBinder(binder).bindJsonCodec(CommitTaskData.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        binder.bind(IcebergFileWriterFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IcebergFileWriterFactory.class).withGeneratedName();

        binder.bind(IcebergEnvironmentContext.class).asEagerSingleton();

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(RollbackToSnapshotProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(RegisterTableProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(UnregisterTableProcedure.class).in(Scopes.SINGLETON);

        Multibinder<TableProcedureMetadata> tableProcedures = newSetBinder(binder, TableProcedureMetadata.class);
        tableProcedures.addBinding().toProvider(OptimizeTableProcedure.class).in(Scopes.SINGLETON);
        tableProcedures.addBinding().toProvider(OptimizeManifestsTableProcedure.class).in(Scopes.SINGLETON);
        tableProcedures.addBinding().toProvider(DropExtendedStatsTableProcedure.class).in(Scopes.SINGLETON);
        tableProcedures.addBinding().toProvider(RollbackToSnapshotTableProcedure.class).in(Scopes.SINGLETON);
        tableProcedures.addBinding().toProvider(ExpireSnapshotsTableProcedure.class).in(Scopes.SINGLETON);
        tableProcedures.addBinding().toProvider(RemoveOrphanFilesTableProcedure.class).in(Scopes.SINGLETON);
        tableProcedures.addBinding().toProvider(AddFilesTableProcedure.class).in(Scopes.SINGLETON);
        tableProcedures.addBinding().toProvider(AddFilesTableFromTableProcedure.class).in(Scopes.SINGLETON);

        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(TableChangesFunctionProvider.class).in(Scopes.SINGLETON);
        binder.bind(FunctionProvider.class).to(IcebergFunctionProvider.class).in(Scopes.SINGLETON);
        binder.bind(TableChangesFunctionProcessorProviderFactory.class).in(Scopes.SINGLETON);

        newSetBinder(binder, SystemTable.class)
                .addBinding()
                .to(IcebergTablesSystemTable.class)
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, IcebergFileSystemFactory.class).setDefault().to(DefaultIcebergFileSystemFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, CacheKeyProvider.class).setBinding().to(IcebergCacheKeyProvider.class).in(Scopes.SINGLETON);

        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForIcebergMetadata.class));
        closingBinder(binder).registerExecutor(Key.get(ListeningExecutorService.class, ForIcebergSplitManager.class));
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForIcebergScanPlanning.class));

        binder.bind(IcebergConnector.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    @ForIcebergMetadata
    public ExecutorService createIcebergMetadataExecutor(CatalogName catalogName)
    {
        return newCachedThreadPool(daemonThreadsNamed("iceberg-metadata-" + catalogName + "-%s"));
    }

    @Provides
    @Singleton
    @ForIcebergSplitManager
    public ListeningExecutorService createSplitSourceExecutor(CatalogName catalogName)
    {
        return listeningDecorator(newCachedThreadPool(daemonThreadsNamed("iceberg-split-source-" + catalogName + "-%s")));
    }

    @Provides
    @Singleton
    @ForIcebergScanPlanning
    public ExecutorService createScanPlanningExecutor(CatalogName catalogName, IcebergConfig config)
    {
        if (config.getSplitManagerThreads() == 0) {
            return newDirectExecutorService();
        }
        return newFixedThreadPool(
                config.getSplitManagerThreads(),
                daemonThreadsNamed("iceberg-split-manager-" + catalogName + "-%s"));
    }
}
