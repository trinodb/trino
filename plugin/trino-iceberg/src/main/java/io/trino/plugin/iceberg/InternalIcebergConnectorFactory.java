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

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.hdfs.HdfsModule;
import io.trino.hdfs.authentication.HdfsAuthenticationModule;
import io.trino.hdfs.azure.HiveAzureModule;
import io.trino.hdfs.gcs.HiveGcsModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.catalog.IcebergCatalogModule;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;

public final class InternalIcebergConnectorFactory
{
    private InternalIcebergConnectorFactory() {}

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Module module,
            Optional<Module> icebergCatalogModule,
            Optional<TrinoFileSystemFactory> fileSystemFactory)
    {
        ClassLoader classLoader = InternalIcebergConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new ConnectorObjectNameGeneratorModule("io.trino.plugin.iceberg", "trino.plugin.iceberg"),
                    new JsonModule(),
                    new IcebergModule(),
                    new IcebergSecurityModule(),
                    icebergCatalogModule.orElse(new IcebergCatalogModule()),
                    new HdfsModule(),
                    new HiveGcsModule(),
                    new HiveAzureModule(),
                    new HdfsAuthenticationModule(),
                    new MBeanServerModule(),
                    fileSystemFactory
                            .map(factory -> (Module) binder -> binder.bind(TrinoFileSystemFactory.class).toInstance(factory))
                            .orElseGet(FileSystemModule::new),
                    binder -> {
                        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                        binder.bind(Tracer.class).toInstance(context.getTracer());
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
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
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            Set<SessionPropertiesProvider> sessionPropertiesProviders = injector.getInstance(Key.get(new TypeLiteral<Set<SessionPropertiesProvider>>() {}));
            IcebergTableProperties icebergTableProperties = injector.getInstance(IcebergTableProperties.class);
            IcebergMaterializedViewAdditionalProperties materializedViewAdditionalProperties = injector.getInstance(IcebergMaterializedViewAdditionalProperties.class);
            IcebergAnalyzeProperties icebergAnalyzeProperties = injector.getInstance(IcebergAnalyzeProperties.class);
            Set<Procedure> procedures = injector.getInstance(Key.get(new TypeLiteral<Set<Procedure>>() {}));
            Set<TableProcedureMetadata> tableProcedures = injector.getInstance(Key.get(new TypeLiteral<Set<TableProcedureMetadata>>() {}));
            Optional<ConnectorAccessControl> accessControl = injector.getInstance(Key.get(new TypeLiteral<Optional<ConnectorAccessControl>>() {}));
            // Materialized view should allow configuring all the supported iceberg table properties for the storage table
            List<PropertyMetadata<?>> materializedViewProperties = Stream.of(icebergTableProperties.getTableProperties(), materializedViewAdditionalProperties.getMaterializedViewProperties())
                    .flatMap(Collection::stream)
                    .collect(toImmutableList());

            return new IcebergConnector(
                    injector,
                    lifeCycleManager,
                    transactionManager,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    sessionPropertiesProviders,
                    IcebergSchemaProperties.SCHEMA_PROPERTIES,
                    icebergTableProperties.getTableProperties(),
                    materializedViewProperties,
                    icebergAnalyzeProperties.getAnalyzeProperties(),
                    accessControl,
                    procedures,
                    tableProcedures);
        }
    }
}
