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
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.security.ConnectorAccessControlModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.deltalake.functions.tablechanges.TableChangesFunctionProvider;
import io.trino.plugin.deltalake.functions.tablechanges.TableChangesProcessorProvider;
import io.trino.plugin.deltalake.procedure.DropExtendedStatsProcedure;
import io.trino.plugin.deltalake.procedure.FlushMetadataCacheProcedure;
import io.trino.plugin.deltalake.procedure.OptimizeTableProcedure;
import io.trino.plugin.deltalake.procedure.RegisterTableProcedure;
import io.trino.plugin.deltalake.procedure.UnregisterTableProcedure;
import io.trino.plugin.deltalake.procedure.VacuumProcedure;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess.ForCachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.ExtendedStatistics;
import io.trino.plugin.deltalake.statistics.ExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.MetaDirStatisticsAccess;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.plugin.deltalake.transactionlog.writer.NoIsolationSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizerManager;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.PropertiesSystemTableProvider;
import io.trino.plugin.hive.SystemTableProvider;
import io.trino.plugin.hive.TransactionalMetadata;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.TranslateHiveViews;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;

import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.plugin.deltalake.DeltaLakeAccessControlMetadataFactory.SYSTEM;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DeltaLakeModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        Provider<CatalogName> catalogName = binder.getProvider(CatalogName.class);

        configBinder(binder).bindConfig(DeltaLakeConfig.class);
        binder.bind(Key.get(boolean.class, TranslateHiveViews.class)).toInstance(false);
        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);

        install(new ConnectorAccessControlModule());
        newOptionalBinder(binder, DeltaLakeAccessControlMetadataFactory.class).setDefault().toInstance(SYSTEM);

        Multibinder<SystemTableProvider> systemTableProviders = newSetBinder(binder, SystemTableProvider.class);
        systemTableProviders.addBinding().to(PropertiesSystemTableProvider.class).in(Scopes.SINGLETON);

        newSetBinder(binder, SessionPropertiesProvider.class)
                .addBinding().to(DeltaLakeSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(DeltaLakeTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(DeltaLakeAnalyzeProperties.class).in(Scopes.SINGLETON);

        binder.bind(DeltaLakeTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(DeltaLakeSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSourceProvider.class)
                .setDefault().to(DeltaLakePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(DeltaLakePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(DeltaLakeNodePartitioningProvider.class).in(Scopes.SINGLETON);

        binder.bind(DeltaLakeMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(CachingExtendedStatisticsAccess.class).in(Scopes.SINGLETON);
        binder.bind(ExtendedStatisticsAccess.class).to(CachingExtendedStatisticsAccess.class).in(Scopes.SINGLETON);
        binder.bind(ExtendedStatisticsAccess.class).annotatedWith(ForCachingExtendedStatisticsAccess.class).to(MetaDirStatisticsAccess.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(ExtendedStatistics.class);
        binder.bind(CheckpointSchemaManager.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(LastCheckpoint.class);
        binder.bind(CheckpointWriterManager.class).in(Scopes.SINGLETON);
        binder.bind(TransactionLogAccess.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TransactionLogAccess.class)
                .as(generator -> generator.generatedNameOf(TransactionLogAccess.class, catalogName.get().toString()));

        binder.bind(TransactionLogWriterFactory.class).in(Scopes.SINGLETON);
        binder.bind(TransactionLogSynchronizerManager.class).in(Scopes.SINGLETON);
        binder.bind(NoIsolationSynchronizer.class).in(Scopes.SINGLETON);
        newMapBinder(binder, String.class, TransactionLogSynchronizer.class);

        newOptionalBinder(binder, DeltaLakeRedirectionsProvider.class)
                .setDefault().toInstance(DeltaLakeRedirectionsProvider.NOOP);

        jsonCodecBinder(binder).bindJsonCodec(DataFileInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(DeltaLakeUpdateResult.class);
        jsonCodecBinder(binder).bindJsonCodec(DeltaLakeMergeResult.class);
        binder.bind(DeltaLakeWriterStats.class).in(Scopes.SINGLETON);
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class)
                .as(generator -> generator.generatedNameOf(FileFormatDataSourceStats.class, catalogName.get().toString()));

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(DropExtendedStatsProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(VacuumProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(RegisterTableProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(UnregisterTableProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(FlushMetadataCacheProcedure.class).in(Scopes.SINGLETON);

        Multibinder<TableProcedureMetadata> tableProcedures = newSetBinder(binder, TableProcedureMetadata.class);
        tableProcedures.addBinding().toProvider(OptimizeTableProcedure.class).in(Scopes.SINGLETON);

        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(TableChangesFunctionProvider.class).in(Scopes.SINGLETON);
        binder.bind(FunctionProvider.class).to(DeltaLakeFunctionProvider.class).in(Scopes.SINGLETON);
        binder.bind(TableChangesProcessorProvider.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    public TransactionalMetadataFactory createTransactionalMetadataFactory()
    {
        // fake implementation of TransactionalMetadataFactory used by HiveTransactionManager.
        // not used in Delta lake connector.
        return (identity, autoCommit) -> new TransactionalMetadata()
        {
            @Override
            public SemiTransactionalHiveMetastore getMetastore()
            {
                throw new RuntimeException("SemiTransactionalHiveMetastore is not used by Delta");
            }

            @Override
            public DirectoryLister getDirectoryLister()
            {
                throw new RuntimeException("DirectoryLister is not used by Delta");
            }

            @Override
            public void commit() {}

            @Override
            public void rollback() {}
        };
    }

    @Singleton
    @Provides
    public ExecutorService createDeltaLakeExecutor(CatalogName catalogName)
    {
        return newCachedThreadPool(daemonThreadsNamed("delta-" + catalogName + "-%s"));
    }
}
