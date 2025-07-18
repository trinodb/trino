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
package io.trino.plugin.lakehouse;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.deltalake.DataFileInfo;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeExecutorModule;
import io.trino.plugin.deltalake.DeltaLakeMergeResult;
import io.trino.plugin.deltalake.DeltaLakeMetadataFactory;
import io.trino.plugin.deltalake.DeltaLakeNodePartitioningProvider;
import io.trino.plugin.deltalake.DeltaLakePageSinkProvider;
import io.trino.plugin.deltalake.DeltaLakePageSourceProvider;
import io.trino.plugin.deltalake.DeltaLakeSessionProperties;
import io.trino.plugin.deltalake.DeltaLakeSplitManager;
import io.trino.plugin.deltalake.DeltaLakeSynchronizerModule;
import io.trino.plugin.deltalake.DeltaLakeTableProperties;
import io.trino.plugin.deltalake.DeltaLakeTransactionManager;
import io.trino.plugin.deltalake.DeltaLakeWriterStats;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreModule;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler;
import io.trino.plugin.deltalake.metastore.file.DeltaLakeFileMetastoreModule;
import io.trino.plugin.deltalake.metastore.glue.DeltaLakeGlueMetastoreModule;
import io.trino.plugin.deltalake.metastore.thrift.DeltaLakeThriftMetastoreModule;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess.ForCachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.ExtendedStatistics;
import io.trino.plugin.deltalake.statistics.ExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.MetaDirStatisticsAccess;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.plugin.deltalake.transactionlog.reader.FileSystemTransactionLogReaderFactory;
import io.trino.plugin.deltalake.transactionlog.reader.TransactionLogReaderFactory;
import io.trino.plugin.deltalake.transactionlog.writer.FileSystemTransactionLogWriterFactory;
import io.trino.plugin.deltalake.transactionlog.writer.NoIsolationSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizerManager;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.hive.metastore.MetastoreTypeConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class LakehouseDeltaModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new DeltaLakeSynchronizerModule());
        install(new DeltaLakeMetastoreModule());

        configBinder(binder).bindConfig(DeltaLakeConfig.class);

        binder.bind(DeltaLakeNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(DeltaLakePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(DeltaLakePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(DeltaLakeSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(DeltaLakeSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DeltaLakeTableProperties.class).in(Scopes.SINGLETON);

        binder.bind(DeltaLakeTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(DeltaLakeMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(DeltaLakeWriterStats.class).in(Scopes.SINGLETON);
        binder.bind(CheckpointSchemaManager.class).in(Scopes.SINGLETON);
        binder.bind(CheckpointWriterManager.class).in(Scopes.SINGLETON);
        binder.bind(TransactionLogAccess.class).in(Scopes.SINGLETON);
        binder.bind(TransactionLogReaderFactory.class).to(FileSystemTransactionLogReaderFactory.class).in(Scopes.SINGLETON);
        binder.bind(TransactionLogWriterFactory.class).to(FileSystemTransactionLogWriterFactory.class).in(Scopes.SINGLETON);
        binder.bind(TransactionLogSynchronizerManager.class).in(Scopes.SINGLETON);
        binder.bind(NoIsolationSynchronizer.class).in(Scopes.SINGLETON);

        binder.bind(CachingExtendedStatisticsAccess.class).in(Scopes.SINGLETON);
        binder.bind(ExtendedStatisticsAccess.class).to(CachingExtendedStatisticsAccess.class).in(Scopes.SINGLETON);
        binder.bind(ExtendedStatisticsAccess.class).annotatedWith(ForCachingExtendedStatisticsAccess.class).to(MetaDirStatisticsAccess.class).in(Scopes.SINGLETON);

        binder.bind(TransactionLogAccess.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TransactionLogAccess.class).withGeneratedName();

        binder.bind(DeltaLakeTableMetadataScheduler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DeltaLakeTableMetadataScheduler.class).withGeneratedName();

        jsonCodecBinder(binder).bindJsonCodec(DataFileInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(DeltaLakeMergeResult.class);
        jsonCodecBinder(binder).bindJsonCodec(ExtendedStatistics.class);
        jsonCodecBinder(binder).bindJsonCodec(LastCheckpoint.class);

        install(switch (buildConfigObject(MetastoreTypeConfig.class).getMetastoreType()) {
            case THRIFT -> new DeltaLakeThriftMetastoreModule();
            case FILE -> new DeltaLakeFileMetastoreModule();
            case GLUE -> new DeltaLakeGlueMetastoreModule();
            case POLARIS -> new DeltaLakeFileMetastoreModule(); // Polaris uses standard HiveMetastore interface
        });

        binder.install(new DeltaLakeExecutorModule());
    }
}
