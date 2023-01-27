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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.trino.filesystem.hdfs.HdfsFileSystemModule;
import io.trino.hdfs.HdfsModule;
import io.trino.hdfs.authentication.HdfsAuthenticationModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.CatalogNameModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorAccessControl;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeEventListener;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreModule;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.azure.HiveAzureModule;
import io.trino.plugin.hive.gcs.HiveGcsModule;
import io.trino.plugin.hive.s3.HiveS3Module;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public final class InternalDeltaLakeConnectorFactory
{
    private InternalDeltaLakeConnectorFactory() {}

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Optional<Module> metastoreModule,
            Module module)
    {
        ClassLoader classLoader = InternalDeltaLakeConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new ConnectorObjectNameGeneratorModule("io.trino.plugin.deltalake", "trino.plugin.deltalake"),
                    new JsonModule(),
                    new MBeanServerModule(),
                    new HdfsModule(),
                    new HiveS3Module(),
                    new DeltaLakeS3Module(),
                    new HiveAzureModule(),
                    new DeltaLakeAzureModule(),
                    new HiveGcsModule(),
                    new DeltaLakeGcsModule(),
                    new HdfsAuthenticationModule(),
                    new HdfsFileSystemModule(),
                    new CatalogNameModule(catalogName),
                    metastoreModule.orElse(new DeltaLakeMetastoreModule()),
                    new DeltaLakeModule(),
                    new DeltaLakeSecurityModule(),
                    binder -> {
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                        newSetBinder(binder, EventListener.class);
                    },
                    module);

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider connectorPageSink = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            Set<SessionPropertiesProvider> sessionPropertiesProviders = injector.getInstance(Key.get(new TypeLiteral<Set<SessionPropertiesProvider>>() {}));
            DeltaLakeTableProperties deltaLakeTableProperties = injector.getInstance(DeltaLakeTableProperties.class);
            DeltaLakeAnalyzeProperties deltaLakeAnalyzeProperties = injector.getInstance(DeltaLakeAnalyzeProperties.class);
            DeltaLakeTransactionManager transactionManager = injector.getInstance(DeltaLakeTransactionManager.class);

            Optional<ConnectorAccessControl> deltaAccessControl = injector.getInstance(Key.get(new TypeLiteral<Optional<ConnectorAccessControl>>() {}))
                    // TODO: add the following when adding support for system tables
                    //   .map(accessControl -> new SystemTableAwareAccessControl(accessControl, systemTableProviders))
                    .map(accessControl -> new ClassLoaderSafeConnectorAccessControl(accessControl, classLoader));

            Set<EventListener> eventListeners = injector.getInstance(Key.get(new TypeLiteral<Set<EventListener>>() {}))
                    .stream()
                    .map(listener -> new ClassLoaderSafeEventListener(listener, classLoader))
                    .collect(toImmutableSet());

            Set<Procedure> procedures = injector.getInstance(Key.get(new TypeLiteral<Set<Procedure>>() {}));
            Set<TableProcedureMetadata> tableProcedures = injector.getInstance(Key.get(new TypeLiteral<Set<TableProcedureMetadata>>() {}));

            return new DeltaLakeConnector(
                    lifeCycleManager,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(connectorPageSink, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    ImmutableSet.of(),
                    procedures,
                    tableProcedures,
                    sessionPropertiesProviders,
                    DeltaLakeSchemaProperties.SCHEMA_PROPERTIES,
                    deltaLakeTableProperties.getTableProperties(),
                    deltaLakeAnalyzeProperties.getAnalyzeProperties(),
                    deltaAccessControl,
                    eventListeners,
                    transactionManager);
        }
    }
}
