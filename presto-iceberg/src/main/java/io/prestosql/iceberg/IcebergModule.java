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
package io.prestosql.iceberg;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.prestosql.plugin.hive.CoercionPolicy;
import io.prestosql.plugin.hive.DirectoryLister;
import io.prestosql.plugin.hive.DynamicConfigurationProvider;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.ForCachingHiveMetastore;
import io.prestosql.plugin.hive.ForHiveClient;
import io.prestosql.plugin.hive.HadoopDirectoryLister;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveClientConfig;
import io.prestosql.plugin.hive.HiveCoercionPolicy;
import io.prestosql.plugin.hive.HiveConnectorId;
import io.prestosql.plugin.hive.HiveFileWriterFactory;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.HiveLocationService;
import io.prestosql.plugin.hive.HiveNodePartitioningProvider;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveTableProperties;
import io.prestosql.plugin.hive.HiveTransactionHandle;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.LocationService;
import io.prestosql.plugin.hive.NamenodeStats;
import io.prestosql.plugin.hive.OrcFileWriterConfig;
import io.prestosql.plugin.hive.ParquetFileWriterConfig;
import io.prestosql.plugin.hive.PartitionUpdate;
import io.prestosql.plugin.hive.RcFileFileWriterFactory;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.HiveMetastoreClientFactory;
import io.prestosql.plugin.hive.metastore.thrift.StaticMetastoreConfig;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastoreConfig;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory;
import io.prestosql.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.prestosql.plugin.hive.s3.HiveS3Config;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

/**
 * Created by parth on 2/3/18.
 */
public class IcebergModule
        implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;
    private final NodeManager nodeManager;
    private final PageIndexerFactory pageIndexerFactory;

    public IcebergModule(String connectorId, TypeManager typeManager, PageIndexerFactory pageIndexerFactory, NodeManager nodeManager)
    {
        this.connectorId = connectorId;
        this.typeManager = typeManager;
        this.nodeManager = nodeManager;
        this.pageIndexerFactory = pageIndexerFactory;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(HiveConnectorId.class).toInstance(new HiveConnectorId(connectorId));
        binder.bind(TypeTranslator.class).toInstance(new HiveTypeTranslator());
        binder.bind(CoercionPolicy.class).to(HiveCoercionPolicy.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        binder.bind(DirectoryLister.class).to(HadoopDirectoryLister.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(HiveClientConfig.class);
        configBinder(binder).bindConfig(HiveS3Config.class);
        binder.bind(HiveSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(NamenodeStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NamenodeStats.class).as(generatedNameOf(NamenodeStats.class, connectorId));
        binder.bind(HiveMetastoreClientFactory.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticMetastoreConfig.class);
        configBinder(binder).bindConfig(OrcFileWriterConfig.class);
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(PageIndexerFactory.class).toInstance(pageIndexerFactory);
        binder.bind(ConnectorSplitManager.class).to(IcebergSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(IcebergPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(IcebergPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        binder.bind(IcebergUtil.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfigurationInitializer.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ParquetFileWriterConfig.class);
        configBinder(binder).bindConfig(ThriftHiveMetastoreConfig.class);
        newSetBinder(binder, DynamicConfigurationProvider.class);

        binder.bind(LocationService.class).to(HiveLocationService.class).in(Scopes.SINGLETON);
        binder.bind(IcebergMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(HiveTransactionManager.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(PartitionUpdate.class);
        jsonCodecBinder(binder).bindJsonCodec(CommitTaskData.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).as(generatedNameOf(FileFormatDataSourceStats.class, connectorId));
        Multibinder<HivePageSourceFactory> pageSourceFactoryBinder = newSetBinder(binder, HivePageSourceFactory.class);
        pageSourceFactoryBinder.addBinding().to(OrcPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(ParquetPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(RcFilePageSourceFactory.class).in(Scopes.SINGLETON);
        Multibinder<HiveFileWriterFactory> fileWriterFactoryBinder = newSetBinder(binder, HiveFileWriterFactory.class);
        fileWriterFactoryBinder.addBinding().to(RcFileFileWriterFactory.class).in(Scopes.SINGLETON);
    }

    @ForHiveClient
    @Singleton
    @Provides
    public ExecutorService createHiveClientExecutor(HiveConnectorId hiveClientId)
    {
        return newCachedThreadPool(daemonThreadsNamed("iceberg-" + hiveClientId + "-%s"));
    }

    @Singleton
    @Provides
    public Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> createMetastoreGetter(HiveTransactionManager transactionManager)
    {
        return transactionHandle -> ((IcebergMetadata) transactionManager.get(transactionHandle)).getMetastore();
    }

    @ForCachingHiveMetastore
    @Singleton
    @Provides
    public ExecutorService createCachingHiveMetastoreExecutor(HiveClientConfig hiveClientConfig)
    {
        return newFixedThreadPool(
                hiveClientConfig.getMaxMetastoreRefreshThreads(),
                daemonThreadsNamed("hive-metastore-iceberg-%s"));
    }
}
