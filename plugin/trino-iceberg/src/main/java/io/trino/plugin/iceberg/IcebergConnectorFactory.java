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

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProviderFactory;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.catalog.IcebergCatalogModule;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class IcebergConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "iceberg";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        return createConnector(catalogName, config, context, EMPTY_MODULE, Optional.empty());
    }

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Module module,
            Optional<Module> icebergCatalogModule)
    {
        ClassLoader classLoader = IcebergConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new ConnectorObjectNameGeneratorModule("io.trino.plugin.iceberg", "trino.plugin.iceberg"),
                    new JsonModule(),
                    new IcebergModule(),
                    new IcebergSecurityModule(),
                    icebergCatalogModule.orElse(new IcebergCatalogModule()),
                    new MBeanServerModule(),
                    new IcebergFileSystemModule(catalogName, context),
                    binder -> {
                        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                        binder.bind(Tracer.class).toInstance(context.getTracer());
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                        binder.bind(CatalogHandle.class).toInstance(context.getCatalogHandle());
                        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                        binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                    },
                    module);

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            IcebergTransactionManager transactionManager = injector.getInstance(IcebergTransactionManager.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProviderFactory connectorPageSource = injector.getInstance(ConnectorPageSourceProviderFactory.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            Set<SessionPropertiesProvider> sessionPropertiesProviders = injector.getInstance(new Key<>() {});
            IcebergTableProperties icebergTableProperties = injector.getInstance(IcebergTableProperties.class);
            IcebergMaterializedViewProperties materializedViewProperties = injector.getInstance(IcebergMaterializedViewProperties.class);
            IcebergAnalyzeProperties icebergAnalyzeProperties = injector.getInstance(IcebergAnalyzeProperties.class);
            Set<Procedure> procedures = injector.getInstance(new Key<>() {});
            Set<TableProcedureMetadata> tableProcedures = injector.getInstance(new Key<>() {});
            Set<ConnectorTableFunction> tableFunctions = injector.getInstance(new Key<>() {});
            FunctionProvider functionProvider = injector.getInstance(FunctionProvider.class);
            Optional<ConnectorAccessControl> accessControl = injector.getInstance(new Key<>() {});

            verify(!injector.getBindings().containsKey(Key.get(HiveConfig.class)), "HiveConfig should not be bound");

            return new IcebergConnector(
                    injector,
                    lifeCycleManager,
                    transactionManager,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProviderFactory(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    sessionPropertiesProviders,
                    IcebergSchemaProperties.SCHEMA_PROPERTIES,
                    icebergTableProperties.getTableProperties(),
                    materializedViewProperties.getMaterializedViewProperties(),
                    icebergAnalyzeProperties.getAnalyzeProperties(),
                    accessControl,
                    procedures,
                    tableProcedures,
                    tableFunctions,
                    functionProvider);
        }
    }

    private static class IcebergFileSystemModule
            extends AbstractConfigurationAwareModule
    {
        private final String catalogName;
        private final NodeManager nodeManager;
        private final OpenTelemetry openTelemetry;

        public IcebergFileSystemModule(String catalogName, ConnectorContext context)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.nodeManager = context.getNodeManager();
            this.openTelemetry = context.getOpenTelemetry();
        }

        @Override
        protected void setup(Binder binder)
        {
            boolean metadataCacheEnabled = buildConfigObject(IcebergConfig.class).isMetadataCacheEnabled();
            install(new FileSystemModule(catalogName, nodeManager, openTelemetry, metadataCacheEnabled));
        }
    }
}
