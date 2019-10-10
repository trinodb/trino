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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.plugin.base.security.AllowAllAccessControl;
import io.prestosql.plugin.hive.HiveCatalogName;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.authentication.HiveAuthenticationModule;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.HiveMetastoreModule;
import io.prestosql.plugin.hive.s3.HiveS3Module;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.prestosql.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;

public final class InternalIcebergConnectorFactory
{
    private InternalIcebergConnectorFactory() {}

    public static Connector createConnector(String catalogName, Map<String, String> config, ConnectorContext context, Optional<HiveMetastore> metastore)
    {
        ClassLoader classLoader = InternalIcebergConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new IcebergModule(),
                    new HiveS3Module(),
                    new HiveAuthenticationModule(),
                    new HiveMetastoreModule(metastore),
                    new MBeanServerModule(),
                    binder -> {
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                        binder.bind(HiveCatalogName.class).toInstance(new HiveCatalogName(catalogName));
                    });

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            IcebergTransactionManager transactionManager = injector.getInstance(IcebergTransactionManager.class);
            IcebergMetadataFactory metadataFactory = injector.getInstance(IcebergMetadataFactory.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            IcebergSessionProperties icebergSessionProperties = injector.getInstance(IcebergSessionProperties.class);
            IcebergTableProperties icebergTableProperties = injector.getInstance(IcebergTableProperties.class);

            return new IcebergConnector(
                    lifeCycleManager,
                    transactionManager,
                    metadataFactory,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    ImmutableSet.of(),
                    icebergSessionProperties.getSessionProperties(),
                    IcebergSchemaProperties.SCHEMA_PROPERTIES,
                    icebergTableProperties.getTableProperties(),
                    new AllowAllAccessControl());
        }
    }
}
