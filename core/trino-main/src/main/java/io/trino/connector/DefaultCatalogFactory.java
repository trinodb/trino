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

import io.airlift.node.NodeInfo;
import io.trino.connector.informationschema.InformationSchemaConnector;
import io.trino.connector.system.CoordinatorSystemTablesProvider;
import io.trino.connector.system.StaticSystemTablesProvider;
import io.trino.connector.system.SystemConnector;
import io.trino.connector.system.SystemTablesProvider;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.server.PluginClassLoader;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;
import io.trino.transaction.TransactionManager;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.connector.CatalogHandle.createInformationSchemaCatalogHandle;
import static io.trino.spi.connector.CatalogHandle.createSystemTablesCatalogHandle;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DefaultCatalogFactory
        implements CatalogFactory
{
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final HandleResolver handleResolver;

    private final InternalNodeManager nodeManager;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final NodeInfo nodeInfo;
    private final VersionEmbedder versionEmbedder;
    private final TransactionManager transactionManager;
    private final TypeManager typeManager;

    private final boolean schedulerIncludeCoordinator;

    private final ConcurrentMap<ConnectorName, InternalConnectorFactory> connectorFactories = new ConcurrentHashMap<>();

    @Inject
    public DefaultCatalogFactory(
            Metadata metadata,
            AccessControl accessControl,
            HandleResolver handleResolver,
            InternalNodeManager nodeManager,
            PageSorter pageSorter,
            PageIndexerFactory pageIndexerFactory,
            NodeInfo nodeInfo,
            VersionEmbedder versionEmbedder,
            TransactionManager transactionManager,
            TypeManager typeManager,
            NodeSchedulerConfig nodeSchedulerConfig)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.schedulerIncludeCoordinator = nodeSchedulerConfig.isIncludeCoordinator();
    }

    @Override
    public synchronized void addConnectorFactory(ConnectorFactory connectorFactory, Function<CatalogHandle, ClassLoader> duplicatePluginClassLoaderFactory)
    {
        InternalConnectorFactory existingConnectorFactory = connectorFactories.putIfAbsent(
                new ConnectorName(connectorFactory.getName()),
                new InternalConnectorFactory(connectorFactory, duplicatePluginClassLoaderFactory));
        checkArgument(existingConnectorFactory == null, "Connector '%s' is already registered", connectorFactory.getName());
    }

    @Override
    public CatalogConnector createCatalog(CatalogProperties catalogProperties)
    {
        requireNonNull(catalogProperties, "catalogProperties is null");

        InternalConnectorFactory factory = connectorFactories.get(catalogProperties.getConnectorName());
        checkArgument(factory != null, "No factory for connector '%s'.  Available factories: %s", catalogProperties.getConnectorName(), connectorFactories.keySet());

        CatalogClassLoaderSupplier duplicatePluginClassLoaderFactory = new CatalogClassLoaderSupplier(
                catalogProperties.getCatalogHandle(),
                factory.getDuplicatePluginClassLoaderFactory(),
                handleResolver);
        Connector connector = createConnector(
                catalogProperties.getCatalogHandle().getCatalogName(),
                catalogProperties.getCatalogHandle(),
                factory.getConnectorFactory(),
                duplicatePluginClassLoaderFactory,
                catalogProperties.getProperties());
        return createCatalog(
                catalogProperties.getCatalogHandle(),
                catalogProperties.getConnectorName(),
                connector,
                duplicatePluginClassLoaderFactory::destroy,
                Optional.of(catalogProperties));
    }

    @Override
    public CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector)
    {
        return createCatalog(catalogHandle, connectorName, connector, () -> {}, Optional.empty());
    }

    private CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector, Runnable destroy, Optional<CatalogProperties> catalogProperties)
    {
        ConnectorServices catalogConnector = new ConnectorServices(
                catalogHandle,
                connector,
                destroy);

        ConnectorServices informationSchemaConnector = new ConnectorServices(
                createInformationSchemaCatalogHandle(catalogHandle),
                new InformationSchemaConnector(catalogHandle.getCatalogName(), nodeManager, metadata, accessControl),
                () -> {});

        SystemTablesProvider systemTablesProvider;
        if (nodeManager.getCurrentNode().isCoordinator()) {
            systemTablesProvider = new CoordinatorSystemTablesProvider(
                    transactionManager,
                    metadata,
                    catalogHandle.getCatalogName(),
                    new StaticSystemTablesProvider(catalogConnector.getSystemTables()));
        }
        else {
            systemTablesProvider = new StaticSystemTablesProvider(catalogConnector.getSystemTables());
        }

        ConnectorServices systemConnector = new ConnectorServices(
                createSystemTablesCatalogHandle(catalogHandle),
                new SystemConnector(
                        nodeManager,
                        systemTablesProvider,
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, catalogHandle)),
                () -> {});

        return new CatalogConnector(
                catalogHandle,
                connectorName,
                catalogConnector,
                informationSchemaConnector,
                systemConnector,
                catalogProperties);
    }

    private Connector createConnector(
            String catalogName,
            CatalogHandle catalogHandle,
            ConnectorFactory connectorFactory,
            Supplier<ClassLoader> duplicatePluginClassLoaderFactory,
            Map<String, String> properties)
    {
        ConnectorContext context = new ConnectorContextInstance(
                catalogHandle,
                new ConnectorAwareNodeManager(nodeManager, nodeInfo.getEnvironment(), catalogHandle, schedulerIncludeCoordinator),
                versionEmbedder,
                typeManager,
                new InternalMetadataProvider(metadata, typeManager),
                pageSorter,
                pageIndexerFactory,
                duplicatePluginClassLoaderFactory);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connectorFactory.getClass().getClassLoader())) {
            return connectorFactory.create(catalogName, properties, context);
        }
    }

    private static class InternalConnectorFactory
    {
        private final ConnectorFactory connectorFactory;
        private final Function<CatalogHandle, ClassLoader> duplicatePluginClassLoaderFactory;

        public InternalConnectorFactory(ConnectorFactory connectorFactory, Function<CatalogHandle, ClassLoader> duplicatePluginClassLoaderFactory)
        {
            this.connectorFactory = connectorFactory;
            this.duplicatePluginClassLoaderFactory = duplicatePluginClassLoaderFactory;
        }

        public ConnectorFactory getConnectorFactory()
        {
            return connectorFactory;
        }

        public Function<CatalogHandle, ClassLoader> getDuplicatePluginClassLoaderFactory()
        {
            return duplicatePluginClassLoaderFactory;
        }

        @Override
        public String toString()
        {
            return connectorFactory.getName();
        }
    }

    private static class CatalogClassLoaderSupplier
            implements Supplier<ClassLoader>
    {
        private final CatalogHandle catalogHandle;
        private final Function<CatalogHandle, ClassLoader> duplicatePluginClassLoaderFactory;
        private final HandleResolver handleResolver;

        @GuardedBy("this")
        private boolean destroyed;

        @GuardedBy("this")
        private ClassLoader classLoader;

        public CatalogClassLoaderSupplier(
                CatalogHandle catalogHandle,
                Function<CatalogHandle, ClassLoader> duplicatePluginClassLoaderFactory,
                HandleResolver handleResolver)
        {
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.duplicatePluginClassLoaderFactory = requireNonNull(duplicatePluginClassLoaderFactory, "duplicatePluginClassLoaderFactory is null");
            this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        }

        @Override
        public ClassLoader get()
        {
            ClassLoader classLoader = duplicatePluginClassLoaderFactory.apply(catalogHandle);

            synchronized (this) {
                // we check this after class loader creation because it reduces the complexity of the synchronization, and this shouldn't happen
                checkState(this.classLoader == null, "class loader is already a duplicated for catalog " + catalogHandle);
                checkState(!destroyed, "catalog has been shutdown");
                this.classLoader = classLoader;
            }

            if (classLoader instanceof PluginClassLoader) {
                handleResolver.registerClassLoader((PluginClassLoader) classLoader);
            }
            return classLoader;
        }

        public void destroy()
        {
            ClassLoader classLoader;
            synchronized (this) {
                checkState(!destroyed, "catalog has been shutdown");
                classLoader = this.classLoader;
                destroyed = true;
            }
            if (classLoader instanceof PluginClassLoader) {
                handleResolver.unregisterClassLoader((PluginClassLoader) classLoader);
            }
        }
    }
}
