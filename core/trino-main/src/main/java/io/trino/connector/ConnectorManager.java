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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.trino.connector.informationschema.InformationSchemaConnector;
import io.trino.connector.system.CoordinatorSystemTablesProvider;
import io.trino.connector.system.StaticSystemTablesProvider;
import io.trino.connector.system.SystemConnector;
import io.trino.connector.system.SystemTablesProvider;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.CatalogMetadata.SecurityManagement;
import io.trino.metadata.CatalogProcedures;
import io.trino.metadata.CatalogTableFunctions;
import io.trino.metadata.CatalogTableProcedures;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.server.PluginClassLoader;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorIndexProvider;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.ptf.ArgumentSpecification;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.ptf.TableArgumentSpecification;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TypeManager;
import io.trino.split.RecordPageSourceProvider;
import io.trino.transaction.TransactionManager;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.connector.CatalogName.createInformationSchemaCatalogName;
import static io.trino.connector.CatalogName.createSystemTablesCatalogName;
import static io.trino.metadata.CatalogMetadata.SecurityManagement.CONNECTOR;
import static io.trino.metadata.CatalogMetadata.SecurityManagement.SYSTEM;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ConnectorManager
        implements ConnectorServicesProvider
{
    private static final Logger log = Logger.get(ConnectorManager.class);

    private final Metadata metadata;
    private final CatalogManager catalogManager;
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

    @GuardedBy("this")
    private final ConcurrentMap<String, InternalConnectorFactory> connectorFactories = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final ConcurrentMap<CatalogName, ConnectorServices> connectors = new ConcurrentHashMap<>();

    private final AtomicBoolean stopped = new AtomicBoolean();

    @Inject
    public ConnectorManager(
            Metadata metadata,
            CatalogManager catalogManager,
            AccessControl accessControl,
            HandleResolver handleResolver,
            InternalNodeManager nodeManager,
            NodeInfo nodeInfo,
            VersionEmbedder versionEmbedder,
            PageSorter pageSorter,
            PageIndexerFactory pageIndexerFactory,
            TransactionManager transactionManager,
            TypeManager typeManager,
            NodeSchedulerConfig nodeSchedulerConfig)
    {
        this.metadata = metadata;
        this.catalogManager = catalogManager;
        this.accessControl = accessControl;
        this.handleResolver = handleResolver;
        this.nodeManager = nodeManager;
        this.pageSorter = pageSorter;
        this.pageIndexerFactory = pageIndexerFactory;
        this.nodeInfo = nodeInfo;
        this.versionEmbedder = versionEmbedder;
        this.transactionManager = transactionManager;
        this.typeManager = typeManager;
        this.schedulerIncludeCoordinator = nodeSchedulerConfig.isIncludeCoordinator();
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (stopped.getAndSet(true)) {
            return;
        }

        for (ConnectorServices connector : connectors.values()) {
            connector.shutdown();
        }
        connectors.clear();
    }

    public synchronized void addConnectorFactory(ConnectorFactory connectorFactory, Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory)
    {
        requireNonNull(connectorFactory, "connectorFactory is null");
        requireNonNull(duplicatePluginClassLoaderFactory, "duplicatePluginClassLoaderFactory is null");
        checkState(!stopped.get(), "ConnectorManager is stopped");
        InternalConnectorFactory existingConnectorFactory = connectorFactories.putIfAbsent(
                connectorFactory.getName(),
                new InternalConnectorFactory(connectorFactory, duplicatePluginClassLoaderFactory));
        checkArgument(existingConnectorFactory == null, "Connector '%s' is already registered", connectorFactory.getName());
    }

    @Override
    public synchronized ConnectorServices getConnectorServices(CatalogName catalogName)
    {
        ConnectorServices connectorServices = connectors.get(catalogName);
        checkArgument(connectorServices != null, "No catalog '%s'", catalogName);
        return connectorServices;
    }

    public synchronized CatalogName createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");

        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(properties, "properties is null");

        InternalConnectorFactory connectorFactory = connectorFactories.get(connectorName);
        checkArgument(connectorFactory != null, "No factory for connector '%s'.  Available factories: %s", connectorName, connectorFactories.keySet());

        CatalogName catalog = new CatalogName(catalogName);
        checkState(!connectors.containsKey(catalog), "Catalog '%s' already exists", catalog);

        CatalogClassLoaderSupplier duplicatePluginClassLoaderFactory = new CatalogClassLoaderSupplier(catalog, connectorFactory.getDuplicatePluginClassLoaderFactory(), handleResolver);
        Connector connector = createConnector(catalog, connectorFactory.getConnectorFactory(), duplicatePluginClassLoaderFactory, properties);
        createCatalog(catalog, connectorName, connector, duplicatePluginClassLoaderFactory::destroy);

        return catalog;
    }

    public synchronized void createCatalog(CatalogName catalogName, String connectorName, Connector connector, Runnable destroy)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(connector, "connector is null");
        requireNonNull(destroy, "destroy is null");

        checkState(!connectors.containsKey(catalogName), "Catalog '%s' already exists", catalogName.getCatalogName());
        checkArgument(catalogManager.getCatalog(catalogName.getCatalogName()).isEmpty(), "Catalog '%s' already exists", catalogName.getCatalogName());

        ConnectorServices connectorServices = new ConnectorServices(
                catalogName,
                connector,
                destroy);

        ConnectorServices informationSchemaConnector = new ConnectorServices(
                createInformationSchemaCatalogName(catalogName),
                new InformationSchemaConnector(catalogName.getCatalogName(), nodeManager, metadata, accessControl),
                () -> {});

        CatalogName systemId = createSystemTablesCatalogName(catalogName);
        SystemTablesProvider systemTablesProvider;

        if (nodeManager.getCurrentNode().isCoordinator()) {
            systemTablesProvider = new CoordinatorSystemTablesProvider(
                    transactionManager,
                    metadata,
                    catalogName.getCatalogName(),
                    new StaticSystemTablesProvider(connectorServices.getSystemTables()));
        }
        else {
            systemTablesProvider = new StaticSystemTablesProvider(connectorServices.getSystemTables());
        }

        ConnectorServices systemConnector = new ConnectorServices(systemId, new SystemConnector(
                nodeManager,
                systemTablesProvider,
                transactionId -> transactionManager.getConnectorTransaction(transactionId, catalogName)),
                () -> {});

        Catalog catalog = new Catalog(
                catalogName,
                connectorName,
                connectorServices,
                informationSchemaConnector.getCatalogName(),
                informationSchemaConnector,
                systemConnector.getCatalogName(),
                systemConnector);
        try {
            addConnectorInternal(connectorServices);
            addConnectorInternal(informationSchemaConnector);
            addConnectorInternal(systemConnector);
            catalogManager.registerCatalog(catalog);
        }
        catch (Throwable e) {
            catalogManager.removeCatalog(catalog.getCatalogName().getCatalogName());
            removeConnectorInternal(systemConnector.getCatalogName());
            removeConnectorInternal(informationSchemaConnector.getCatalogName());
            removeConnectorInternal(connectorServices.getCatalogName());
            throw e;
        }
    }

    private synchronized void addConnectorInternal(ConnectorServices connector)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        CatalogName catalogName = connector.getCatalogName();
        checkState(!connectors.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        connectors.put(catalogName, connector);
    }

    private synchronized void removeConnectorInternal(CatalogName catalogName)
    {
        ConnectorServices connectorServices = connectors.remove(catalogName);
        if (connectorServices != null) {
            connectorServices.shutdown();
        }
    }

    private Connector createConnector(
            CatalogName catalogName,
            ConnectorFactory connectorFactory,
            Supplier<ClassLoader> duplicatePluginClassLoaderFactory,
            Map<String, String> properties)
    {
        ConnectorContext context = new ConnectorContextInstance(
                new ConnectorAwareNodeManager(nodeManager, nodeInfo.getEnvironment(), catalogName, schedulerIncludeCoordinator),
                versionEmbedder,
                typeManager,
                new InternalMetadataProvider(metadata, typeManager),
                pageSorter,
                pageIndexerFactory,
                duplicatePluginClassLoaderFactory);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connectorFactory.getClass().getClassLoader())) {
            return connectorFactory.create(catalogName.getCatalogName(), properties, context);
        }
    }

    private static class InternalConnectorFactory
    {
        private final ConnectorFactory connectorFactory;
        private final Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory;

        public InternalConnectorFactory(ConnectorFactory connectorFactory, Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory)
        {
            this.connectorFactory = connectorFactory;
            this.duplicatePluginClassLoaderFactory = duplicatePluginClassLoaderFactory;
        }

        public ConnectorFactory getConnectorFactory()
        {
            return connectorFactory;
        }

        public Function<CatalogName, ClassLoader> getDuplicatePluginClassLoaderFactory()
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
        private final CatalogName catalogName;
        private final Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory;
        private final HandleResolver handleResolver;

        @GuardedBy("this")
        private boolean destroyed;

        @GuardedBy("this")
        private ClassLoader classLoader;

        public CatalogClassLoaderSupplier(
                CatalogName catalogName,
                Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory,
                HandleResolver handleResolver)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.duplicatePluginClassLoaderFactory = requireNonNull(duplicatePluginClassLoaderFactory, "duplicatePluginClassLoaderFactory is null");
            this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        }

        @Override
        public ClassLoader get()
        {
            ClassLoader classLoader = duplicatePluginClassLoaderFactory.apply(catalogName);

            synchronized (this) {
                // we check this after class loader creation because it reduces the complexity of the synchronization, and this shouldn't happen
                checkState(this.classLoader == null, "class loader is already a duplicated for catalog " + catalogName);
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

    public static class ConnectorServices
    {
        private final CatalogName catalogName;
        private final Connector connector;
        private final Runnable afterShutdown;
        private final Set<SystemTable> systemTables;
        private final CatalogProcedures procedures;
        private final CatalogTableProcedures tableProcedures;
        private final CatalogTableFunctions tableFunctions;
        private final Optional<ConnectorSplitManager> splitManager;
        private final Optional<ConnectorPageSourceProvider> pageSourceProvider;
        private final Optional<ConnectorPageSinkProvider> pageSinkProvider;
        private final Optional<ConnectorIndexProvider> indexProvider;
        private final Optional<ConnectorNodePartitioningProvider> partitioningProvider;
        private final Optional<ConnectorAccessControl> accessControl;
        private final List<EventListener> eventListeners;
        private final Map<String, PropertyMetadata<?>> sessionProperties;
        private final Map<String, PropertyMetadata<?>> tableProperties;
        private final Map<String, PropertyMetadata<?>> materializedViewProperties;
        private final Map<String, PropertyMetadata<?>> schemaProperties;
        private final Map<String, PropertyMetadata<?>> columnProperties;
        private final Map<String, PropertyMetadata<?>> analyzeProperties;
        private final Set<ConnectorCapabilities> capabilities;

        public ConnectorServices(CatalogName catalogName, Connector connector, Runnable afterShutdown)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.connector = requireNonNull(connector, "connector is null");
            this.afterShutdown = requireNonNull(afterShutdown, "afterShutdown is null");

            Set<SystemTable> systemTables = connector.getSystemTables();
            requireNonNull(systemTables, format("Connector '%s' returned a null system tables set", catalogName));
            this.systemTables = ImmutableSet.copyOf(systemTables);

            Set<Procedure> procedures = connector.getProcedures();
            requireNonNull(procedures, format("Connector '%s' returned a null procedures set", catalogName));
            this.procedures = new CatalogProcedures(procedures);

            Set<TableProcedureMetadata> tableProcedures = connector.getTableProcedures();
            requireNonNull(tableProcedures, format("Connector '%s' returned a null table procedures set", catalogName));
            this.tableProcedures = new CatalogTableProcedures(tableProcedures);

            Set<ConnectorTableFunction> tableFunctions = connector.getTableFunctions();
            requireNonNull(tableFunctions, format("Connector '%s' returned a null table functions set", catalogName));
            // TODO ConnectorTableFunction should be converted to a metadata class (and a separate analysis interface) which performs this validation in the constructor
            this.tableFunctions = new CatalogTableFunctions(tableFunctions);
            tableFunctions.forEach(ConnectorServices::validateTableFunction);

            ConnectorSplitManager splitManager = null;
            try {
                splitManager = connector.getSplitManager();
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.splitManager = Optional.ofNullable(splitManager);

            ConnectorPageSourceProvider connectorPageSourceProvider = null;
            try {
                connectorPageSourceProvider = connector.getPageSourceProvider();
                requireNonNull(connectorPageSourceProvider, format("Connector '%s' returned a null page source provider", catalogName));
            }
            catch (UnsupportedOperationException ignored) {
            }

            try {
                ConnectorRecordSetProvider connectorRecordSetProvider = connector.getRecordSetProvider();
                requireNonNull(connectorRecordSetProvider, format("Connector '%s' returned a null record set provider", catalogName));
                verify(connectorPageSourceProvider == null, "Connector '%s' returned both page source and record set providers", catalogName);
                connectorPageSourceProvider = new RecordPageSourceProvider(connectorRecordSetProvider);
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.pageSourceProvider = Optional.ofNullable(connectorPageSourceProvider);

            ConnectorPageSinkProvider connectorPageSinkProvider = null;
            try {
                connectorPageSinkProvider = connector.getPageSinkProvider();
                requireNonNull(connectorPageSinkProvider, format("Connector '%s' returned a null page sink provider", catalogName));
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.pageSinkProvider = Optional.ofNullable(connectorPageSinkProvider);

            ConnectorIndexProvider indexProvider = null;
            try {
                indexProvider = connector.getIndexProvider();
                requireNonNull(indexProvider, format("Connector '%s' returned a null index provider", catalogName));
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.indexProvider = Optional.ofNullable(indexProvider);

            ConnectorNodePartitioningProvider partitioningProvider = null;
            try {
                partitioningProvider = connector.getNodePartitioningProvider();
                requireNonNull(partitioningProvider, format("Connector '%s' returned a null partitioning provider", catalogName));
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.partitioningProvider = Optional.ofNullable(partitioningProvider);

            ConnectorAccessControl accessControl = null;
            try {
                accessControl = connector.getAccessControl();
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.accessControl = Optional.ofNullable(accessControl);

            Iterable<EventListener> eventListeners = connector.getEventListeners();
            requireNonNull(eventListeners, format("Connector '%s' returned a null event listeners iterable", eventListeners));
            this.eventListeners = ImmutableList.copyOf(eventListeners);

            List<PropertyMetadata<?>> sessionProperties = connector.getSessionProperties();
            requireNonNull(sessionProperties, format("Connector '%s' returned a null system properties set", catalogName));
            this.sessionProperties = Maps.uniqueIndex(sessionProperties, PropertyMetadata::getName);

            List<PropertyMetadata<?>> tableProperties = connector.getTableProperties();
            requireNonNull(tableProperties, format("Connector '%s' returned a null table properties set", catalogName));
            this.tableProperties = Maps.uniqueIndex(tableProperties, PropertyMetadata::getName);

            List<PropertyMetadata<?>> materializedViewProperties = connector.getMaterializedViewProperties();
            requireNonNull(materializedViewProperties, format("Connector '%s' returned a null materialized view properties set", catalogName));
            this.materializedViewProperties = Maps.uniqueIndex(materializedViewProperties, PropertyMetadata::getName);

            List<PropertyMetadata<?>> schemaProperties = connector.getSchemaProperties();
            requireNonNull(schemaProperties, format("Connector '%s' returned a null schema properties set", catalogName));
            this.schemaProperties = Maps.uniqueIndex(schemaProperties, PropertyMetadata::getName);

            List<PropertyMetadata<?>> columnProperties = connector.getColumnProperties();
            requireNonNull(columnProperties, format("Connector '%s' returned a null column properties set", catalogName));
            this.columnProperties = Maps.uniqueIndex(columnProperties, PropertyMetadata::getName);

            List<PropertyMetadata<?>> analyzeProperties = connector.getAnalyzeProperties();
            requireNonNull(analyzeProperties, format("Connector '%s' returned a null analyze properties set", catalogName));
            this.analyzeProperties = Maps.uniqueIndex(analyzeProperties, PropertyMetadata::getName);

            Set<ConnectorCapabilities> capabilities = connector.getCapabilities();
            requireNonNull(capabilities, format("Connector '%s' returned a null capabilities set", catalogName));
            this.capabilities = capabilities;
        }

        public CatalogName getCatalogName()
        {
            return catalogName;
        }

        public Connector getConnector()
        {
            return connector;
        }

        public Set<SystemTable> getSystemTables()
        {
            return systemTables;
        }

        public CatalogProcedures getProcedures()
        {
            return procedures;
        }

        public CatalogTableProcedures getTableProcedures()
        {
            return tableProcedures;
        }

        public CatalogTableFunctions getTableFunctions()
        {
            return tableFunctions;
        }

        public Optional<ConnectorSplitManager> getSplitManager()
        {
            return splitManager;
        }

        public Optional<ConnectorPageSourceProvider> getPageSourceProvider()
        {
            return pageSourceProvider;
        }

        public Optional<ConnectorPageSinkProvider> getPageSinkProvider()
        {
            return pageSinkProvider;
        }

        public Optional<ConnectorIndexProvider> getIndexProvider()
        {
            return indexProvider;
        }

        public Optional<ConnectorNodePartitioningProvider> getPartitioningProvider()
        {
            return partitioningProvider;
        }

        public SecurityManagement getSecurityManagement()
        {
            return accessControl.isPresent() ? CONNECTOR : SYSTEM;
        }

        public Optional<ConnectorAccessControl> getAccessControl()
        {
            return accessControl;
        }

        public List<EventListener> getEventListeners()
        {
            return eventListeners;
        }

        public Map<String, PropertyMetadata<?>> getSessionProperties()
        {
            return sessionProperties;
        }

        public Map<String, PropertyMetadata<?>> getTableProperties()
        {
            return tableProperties;
        }

        public Map<String, PropertyMetadata<?>> getMaterializedViewProperties()
        {
            return materializedViewProperties;
        }

        public Map<String, PropertyMetadata<?>> getColumnProperties()
        {
            return columnProperties;
        }

        public Map<String, PropertyMetadata<?>> getSchemaProperties()
        {
            return schemaProperties;
        }

        public Map<String, PropertyMetadata<?>> getAnalyzeProperties()
        {
            return analyzeProperties;
        }

        public Set<ConnectorCapabilities> getCapabilities()
        {
            return capabilities;
        }

        public void shutdown()
        {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connector.getClass().getClassLoader())) {
                connector.shutdown();
            }
            catch (Throwable t) {
                log.error(t, "Error shutting down connector: %s", catalogName);
            }
            finally {
                afterShutdown.run();
            }
        }

        private static void validateTableFunction(ConnectorTableFunction tableFunction)
        {
            requireNonNull(tableFunction, "tableFunction is null");
            requireNonNull(tableFunction.getName(), "table function name is null");
            requireNonNull(tableFunction.getSchema(), "table function schema name is null");
            requireNonNull(tableFunction.getArguments(), "table function arguments is null");
            requireNonNull(tableFunction.getReturnTypeSpecification(), "table function returnTypeSpecification is null");

            checkArgument(!tableFunction.getName().isEmpty(), "table function name is empty");
            checkArgument(!tableFunction.getSchema().isEmpty(), "table function schema name is empty");

            Set<String> argumentNames = new HashSet<>();
            for (ArgumentSpecification specification : tableFunction.getArguments()) {
                if (!argumentNames.add(specification.getName())) {
                    throw new IllegalArgumentException("duplicate argument name: " + specification.getName());
                }
            }
            long tableArgumentsWithRowSemantics = tableFunction.getArguments().stream()
                    .filter(TableArgumentSpecification.class::isInstance)
                    .map(TableArgumentSpecification.class::cast)
                    .filter(TableArgumentSpecification::isRowSemantics)
                    .count();
            checkArgument(tableArgumentsWithRowSemantics <= 1, "more than one table argument with row semantics");
        }
    }
}
