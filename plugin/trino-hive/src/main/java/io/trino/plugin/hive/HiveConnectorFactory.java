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
package io.trino.plugin.hive;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.CatalogNameModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorAccessControl;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreModule;
import io.trino.plugin.hive.procedure.HiveProcedureModule;
import io.trino.plugin.hive.security.HiveSecurityModule;
import io.trino.plugin.hive.security.SystemTableAwareAccessControl;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.procedure.Procedure;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;

public class HiveConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "hive";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        return createConnector(catalogName, config, context, EMPTY_MODULE, Optional.empty(), Optional.empty());
    }

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Module module,
            Optional<HiveMetastore> metastore,
            Optional<TrinoFileSystemFactory> fileSystemFactory)
    {
        ClassLoader classLoader = HiveConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new CatalogNameModule(catalogName),
                    new EventModule(),
                    new MBeanModule(),
                    new ConnectorObjectNameGeneratorModule("io.trino.plugin.hive", "trino.plugin.hive"),
                    new JsonModule(),
                    new TypeDeserializerModule(context.getTypeManager()),
                    new HiveModule(),
                    new HiveMetastoreModule(metastore),
                    new HiveSecurityModule(),
                    fileSystemFactory
                            .map(factory -> (Module) binder -> binder.bind(TrinoFileSystemFactory.class).toInstance(factory))
                            .orElseGet(() -> new FileSystemModule(catalogName, context.getNodeManager(), context.getOpenTelemetry())),
                    new HiveProcedureModule(),
                    new MBeanServerModule(),
                    binder -> {
                        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                        binder.bind(Tracer.class).toInstance(context.getTracer());
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder());
                        binder.bind(MetadataProvider.class).toInstance(context.getMetadataProvider());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                        binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                    },
                    binder -> newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(HiveSessionProperties.class).in(Scopes.SINGLETON),
                    module);

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            HiveTransactionManager transactionManager = injector.getInstance(HiveTransactionManager.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            Set<SessionPropertiesProvider> sessionPropertiesProviders = injector.getInstance(Key.get(new TypeLiteral<>() {}));
            HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
            HiveColumnProperties hiveColumnProperties = injector.getInstance(HiveColumnProperties.class);
            HiveAnalyzeProperties hiveAnalyzeProperties = injector.getInstance(HiveAnalyzeProperties.class);
            HiveMaterializedViewPropertiesProvider hiveMaterializedViewPropertiesProvider = injector.getInstance(HiveMaterializedViewPropertiesProvider.class);
            Set<Procedure> procedures = injector.getInstance(Key.get(new TypeLiteral<>() {}));
            Set<TableProcedureMetadata> tableProcedures = injector.getInstance(Key.get(new TypeLiteral<>() {}));
            Set<SystemTableProvider> systemTableProviders = injector.getInstance(Key.get(new TypeLiteral<>() {}));
            Optional<ConnectorAccessControl> hiveAccessControl = injector.getInstance(Key.get(new TypeLiteral<Optional<ConnectorAccessControl>>() {}))
                    .map(accessControl -> new SystemTableAwareAccessControl(accessControl, systemTableProviders))
                    .map(accessControl -> new ClassLoaderSafeConnectorAccessControl(accessControl, classLoader));

            return new HiveConnector(
                    injector,
                    lifeCycleManager,
                    transactionManager,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    procedures,
                    tableProcedures,
                    sessionPropertiesProviders,
                    HiveSchemaProperties.SCHEMA_PROPERTIES,
                    hiveTableProperties.getTableProperties(),
                    hiveColumnProperties.getColumnProperties(),
                    hiveAnalyzeProperties.getAnalyzeProperties(),
                    hiveMaterializedViewPropertiesProvider.getMaterializedViewProperties(),
                    hiveAccessControl,
                    injector.getInstance(Key.get(new TypeLiteral<>() {})),
                    injector.getInstance(FunctionProvider.class),
                    injector.getInstance(HiveConfig.class).isSingleStatementWritesOnly(),
                    classLoader);
        }
    }
}
