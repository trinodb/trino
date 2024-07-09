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
package io.trino.connector;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.node.NodeInfo;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.connector.informationschema.InformationSchemaConnector;
import io.trino.connector.system.CoordinatorSystemTablesProvider;
import io.trino.connector.system.StaticSystemTablesProvider;
import io.trino.connector.system.SystemConnector;
import io.trino.connector.system.SystemTablesProvider;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.LocalMemoryManager;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.type.TypeManager;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.transaction.TransactionManager;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.connector.CatalogHandle.createInformationSchemaCatalogHandle;
import static io.trino.spi.connector.CatalogHandle.createSystemTablesCatalogHandle;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DefaultCatalogFactory
        implements CatalogFactory
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    private final InternalNodeManager nodeManager;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final NodeInfo nodeInfo;
    private final VersionEmbedder versionEmbedder;
    private final OpenTelemetry openTelemetry;
    private final TransactionManager transactionManager;
    private final TypeManager typeManager;

    private final boolean schedulerIncludeCoordinator;
    private final int maxPrefetchedInformationSchemaPrefixes;

    private final ConcurrentMap<ConnectorName, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();
    private final LocalMemoryManager localMemoryManager;
    private final SecretsResolver secretsResolver;

    @Inject
    public DefaultCatalogFactory(
            Metadata metadata,
            AccessControl accessControl,
            InternalNodeManager nodeManager,
            PageSorter pageSorter,
            PageIndexerFactory pageIndexerFactory,
            NodeInfo nodeInfo,
            VersionEmbedder versionEmbedder,
            OpenTelemetry openTelemetry,
            TransactionManager transactionManager,
            TypeManager typeManager,
            NodeSchedulerConfig nodeSchedulerConfig,
            OptimizerConfig optimizerConfig,
            LocalMemoryManager localMemoryManager,
            SecretsResolver secretsResolver)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.schedulerIncludeCoordinator = nodeSchedulerConfig.isIncludeCoordinator();
        this.maxPrefetchedInformationSchemaPrefixes = optimizerConfig.getMaxPrefetchedInformationSchemaPrefixes();
        this.localMemoryManager = requireNonNull(localMemoryManager, "localMemoryManager is null");
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
    }

    @Override
    public synchronized void addConnectorFactory(ConnectorFactory connectorFactory)
    {
        ConnectorFactory existingConnectorFactory = connectorFactories.putIfAbsent(
                new ConnectorName(connectorFactory.getName()), connectorFactory);
        checkArgument(existingConnectorFactory == null, "Connector '%s' is already registered", connectorFactory.getName());
    }

    @Override
    public CatalogConnector createCatalog(CatalogProperties catalogProperties)
    {
        requireNonNull(catalogProperties, "catalogProperties is null");

        ConnectorFactory connectorFactory = connectorFactories.get(catalogProperties.connectorName());
        checkArgument(connectorFactory != null, "No factory for connector '%s'. Available factories: %s", catalogProperties.connectorName(), connectorFactories.keySet());

        Connector connector = createConnector(
                catalogProperties.catalogHandle().getCatalogName().toString(),
                catalogProperties.catalogHandle(),
                connectorFactory,
                secretsResolver.getResolvedConfiguration(catalogProperties.properties()));

        return createCatalog(
                catalogProperties.catalogHandle(),
                catalogProperties.connectorName(),
                connector,
                Optional.of(catalogProperties));
    }

    @Override
    public CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector)
    {
        return createCatalog(catalogHandle, connectorName, connector, Optional.empty());
    }

    private CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector, Optional<CatalogProperties> catalogProperties)
    {
        Tracer tracer = createTracer(catalogHandle);

        ConnectorServices catalogConnector = new ConnectorServices(tracer, catalogHandle, connector);

        ConnectorServices informationSchemaConnector = new ConnectorServices(
                tracer,
                createInformationSchemaCatalogHandle(catalogHandle),
                new InformationSchemaConnector(
                        catalogHandle.getCatalogName().toString(),
                        nodeManager,
                        metadata,
                        accessControl,
                        maxPrefetchedInformationSchemaPrefixes));

        SystemTablesProvider systemTablesProvider;
        if (nodeManager.getCurrentNode().isCoordinator()) {
            systemTablesProvider = new CoordinatorSystemTablesProvider(
                    transactionManager,
                    metadata,
                    catalogHandle.getCatalogName().toString(),
                    new StaticSystemTablesProvider(catalogConnector.getSystemTables()));
        }
        else {
            systemTablesProvider = new StaticSystemTablesProvider(catalogConnector.getSystemTables());
        }

        ConnectorServices systemConnector = new ConnectorServices(
                tracer,
                createSystemTablesCatalogHandle(catalogHandle),
                new SystemConnector(
                        nodeManager,
                        systemTablesProvider,
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, catalogHandle)));

        return new CatalogConnector(
                catalogHandle,
                connectorName,
                catalogConnector,
                informationSchemaConnector,
                systemConnector,
                localMemoryManager,
                catalogProperties);
    }

    private Connector createConnector(
            String catalogName,
            CatalogHandle catalogHandle,
            ConnectorFactory connectorFactory,
            Map<String, String> properties)
    {
        ConnectorContext context = new ConnectorContextInstance(
                catalogHandle,
                openTelemetry,
                createTracer(catalogHandle),
                new ConnectorAwareNodeManager(nodeManager, nodeInfo.getEnvironment(), catalogHandle, schedulerIncludeCoordinator),
                versionEmbedder,
                typeManager,
                new InternalMetadataProvider(metadata, typeManager),
                pageSorter,
                pageIndexerFactory);

        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(connectorFactory.getClass().getClassLoader())) {
            return connectorFactory.create(catalogName, properties, context);
        }
    }

    private Tracer createTracer(CatalogHandle catalogHandle)
    {
        return openTelemetry.getTracer("trino.catalog." + catalogHandle.getCatalogName());
    }
}
