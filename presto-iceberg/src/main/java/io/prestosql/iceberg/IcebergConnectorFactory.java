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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.prestosql.plugin.hive.HiveSchemaProperties;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveTableProperties;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.RebindSafeMBeanServer;
import io.prestosql.plugin.hive.authentication.HiveAuthenticationModule;
import io.prestosql.plugin.hive.metastore.ExtendedHiveMetastore;
import io.prestosql.plugin.hive.metastore.HiveMetastoreModule;
import io.prestosql.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.HiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastore;
import io.prestosql.plugin.hive.s3.HiveS3Module;
import io.prestosql.plugin.hive.security.HiveSecurityModule;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
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

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class IcebergConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final ClassLoader classLoader;
    private final Optional<ExtendedHiveMetastore> metastore;

    public IcebergConnectorFactory(String name, ClassLoader classLoader, Optional<ExtendedHiveMetastore> metastore)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.metastore = metastore;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new IcebergHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new IcebergModule(connectorId, context.getTypeManager(), context.getPageIndexerFactory(), context.getNodeManager()),
                    new HiveS3Module(),
                    new HiveSecurityModule(),
                    new HiveAuthenticationModule(),
                    new HiveMetastoreModule(metastore),
                    binder -> {
                        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
                        binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(platformMBeanServer));
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(HiveMetastore.class).to(ThriftHiveMetastore.class).in(Scopes.SINGLETON);
                    });

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            IcebergMetadataFactory metadataFactory = injector.getInstance(IcebergMetadataFactory.class);
            HiveTransactionManager transactionManager = injector.getInstance(HiveTransactionManager.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            HiveSessionProperties hiveSessionProperties = injector.getInstance(HiveSessionProperties.class);
            HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
            ConnectorAccessControl accessControl = injector.getInstance(ConnectorAccessControl.class);

            return new IcebergConnector(
                    lifeCycleManager,
                    metadataFactory,
                    transactionManager,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    ImmutableSet.of(),
                    hiveSessionProperties.getSessionProperties(),
                    HiveSchemaProperties.SCHEMA_PROPERTIES,
                    hiveTableProperties.getTableProperties(),
                    accessControl,
                    classLoader);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
