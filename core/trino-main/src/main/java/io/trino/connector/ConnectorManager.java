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
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.trino.connector.informationschema.InformationSchemaConnector;
import io.trino.connector.system.CoordinatorSystemTablesProvider;
import io.trino.connector.system.StaticSystemTablesProvider;
import io.trino.connector.system.SystemConnector;
import io.trino.connector.system.SystemTablesProvider;
import io.trino.eventlistener.EventListenerManager;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.index.IndexManager;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.Catalog;
import io.trino.metadata.Catalog.SecurityManagement;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ProcedureRegistry;
import io.trino.metadata.SchemaPropertyManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TableProceduresPropertyManager;
import io.trino.metadata.TableProceduresRegistry;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AccessControlManager;
import io.trino.server.PluginClassLoader;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
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
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TypeManager;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceManager;
import io.trino.split.RecordPageSourceProvider;
import io.trino.split.SplitManager;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.transaction.TransactionManager;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

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
import static io.trino.metadata.Catalog.SecurityManagement.CONNECTOR;
import static io.trino.metadata.Catalog.SecurityManagement.SYSTEM;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ConnectorManager
{
    private static final Logger log = Logger.get(ConnectorManager.class);

    private final MetadataManager metadataManager;
    private final CatalogManager catalogManager;
    private final AccessControlManager accessControlManager;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final IndexManager indexManager;
    private final NodePartitioningManager nodePartitioningManager;

    private final PageSinkManager pageSinkManager;
    private final HandleResolver handleResolver;
    private final InternalNodeManager nodeManager;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final NodeInfo nodeInfo;
    private final VersionEmbedder versionEmbedder;
    private final TransactionManager transactionManager;
    private final EventListenerManager eventListenerManager;
    private final TypeManager typeManager;
    private final ProcedureRegistry procedureRegistry;
    private final TableProceduresRegistry tableProceduresRegistry;
    private final SessionPropertyManager sessionPropertyManager;
    private final SchemaPropertyManager schemaPropertyManager;
    private final ColumnPropertyManager columnPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final MaterializedViewPropertyManager materializedViewPropertyManager;
    private final AnalyzePropertyManager analyzePropertyManager;
    private final TableProceduresPropertyManager tableProceduresPropertyManager;

    private final boolean schedulerIncludeCoordinator;

    @GuardedBy("this")
    private final ConcurrentMap<String, InternalConnectorFactory> connectorFactories = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final ConcurrentMap<CatalogName, MaterializedConnector> connectors = new ConcurrentHashMap<>();

    private final AtomicBoolean stopped = new AtomicBoolean();

    @Inject
    public ConnectorManager(
            MetadataManager metadataManager,
            CatalogManager catalogManager,
            AccessControlManager accessControlManager,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            IndexManager indexManager,
            NodePartitioningManager nodePartitioningManager,
            PageSinkManager pageSinkManager,
            HandleResolver handleResolver,
            InternalNodeManager nodeManager,
            NodeInfo nodeInfo,
            VersionEmbedder versionEmbedder,
            PageSorter pageSorter,
            PageIndexerFactory pageIndexerFactory,
            TransactionManager transactionManager,
            EventListenerManager eventListenerManager,
            TypeManager typeManager,
            ProcedureRegistry procedureRegistry,
            TableProceduresRegistry tableProceduresRegistry,
            SessionPropertyManager sessionPropertyManager,
            SchemaPropertyManager schemaPropertyManager,
            ColumnPropertyManager columnPropertyManager,
            TablePropertyManager tablePropertyManager,
            MaterializedViewPropertyManager materializedViewPropertyManager,
            AnalyzePropertyManager analyzePropertyManager,
            TableProceduresPropertyManager tableProceduresPropertyManager,
            NodeSchedulerConfig nodeSchedulerConfig)
    {
        this.metadataManager = metadataManager;
        this.catalogManager = catalogManager;
        this.accessControlManager = accessControlManager;
        this.splitManager = splitManager;
        this.pageSourceManager = pageSourceManager;
        this.indexManager = indexManager;
        this.nodePartitioningManager = nodePartitioningManager;
        this.pageSinkManager = pageSinkManager;
        this.handleResolver = handleResolver;
        this.nodeManager = nodeManager;
        this.pageSorter = pageSorter;
        this.pageIndexerFactory = pageIndexerFactory;
        this.nodeInfo = nodeInfo;
        this.versionEmbedder = versionEmbedder;
        this.transactionManager = transactionManager;
        this.eventListenerManager = eventListenerManager;
        this.typeManager = typeManager;
        this.procedureRegistry = procedureRegistry;
        this.tableProceduresRegistry = tableProceduresRegistry;
        this.sessionPropertyManager = sessionPropertyManager;
        this.schemaPropertyManager = schemaPropertyManager;
        this.columnPropertyManager = columnPropertyManager;
        this.tablePropertyManager = tablePropertyManager;
        this.materializedViewPropertyManager = materializedViewPropertyManager;
        this.analyzePropertyManager = analyzePropertyManager;
        this.tableProceduresPropertyManager = tableProceduresPropertyManager;
        this.schedulerIncludeCoordinator = nodeSchedulerConfig.isIncludeCoordinator();
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (stopped.getAndSet(true)) {
            return;
        }

        for (MaterializedConnector connector : connectors.values()) {
            connector.shutdown();
        }
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

    public synchronized CatalogName createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        requireNonNull(connectorName, "connectorName is null");
        InternalConnectorFactory connectorFactory = connectorFactories.get(connectorName);
        checkArgument(connectorFactory != null, "No factory for connector '%s'.  Available factories: %s", connectorName, connectorFactories.keySet());
        return createCatalog(catalogName, connectorName, connectorFactory, properties);
    }

    private synchronized CatalogName createCatalog(String catalogName, String connectorName, InternalConnectorFactory connectorFactory, Map<String, String> properties)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(properties, "properties is null");
        requireNonNull(connectorFactory, "connectorFactory is null");
        checkArgument(catalogManager.getCatalog(catalogName).isEmpty(), "Catalog '%s' already exists", catalogName);

        CatalogName catalog = new CatalogName(catalogName);
        checkState(!connectors.containsKey(catalog), "Catalog '%s' already exists", catalog);

        createCatalog(catalog, connectorName, connectorFactory, properties);

        return catalog;
    }

    private synchronized void createCatalog(CatalogName catalogName, String connectorName, InternalConnectorFactory factory, Map<String, String> properties)
    {
        // create all connectors before adding, so a broken connector does not leave the system half updated
        CatalogClassLoaderSupplier duplicatePluginClassLoaderFactory = new CatalogClassLoaderSupplier(catalogName, factory.getDuplicatePluginClassLoaderFactory(), handleResolver);
        MaterializedConnector connector = new MaterializedConnector(
                catalogName,
                createConnector(catalogName, factory.getConnectorFactory(), duplicatePluginClassLoaderFactory, properties),
                duplicatePluginClassLoaderFactory::destroy);

        MaterializedConnector informationSchemaConnector = new MaterializedConnector(
                createInformationSchemaCatalogName(catalogName),
                new InformationSchemaConnector(catalogName.getCatalogName(), nodeManager, metadataManager, accessControlManager),
                () -> {});

        CatalogName systemId = createSystemTablesCatalogName(catalogName);
        SystemTablesProvider systemTablesProvider;

        if (nodeManager.getCurrentNode().isCoordinator()) {
            systemTablesProvider = new CoordinatorSystemTablesProvider(
                    transactionManager,
                    metadataManager,
                    catalogName.getCatalogName(),
                    new StaticSystemTablesProvider(connector.getSystemTables()));
        }
        else {
            systemTablesProvider = new StaticSystemTablesProvider(connector.getSystemTables());
        }

        MaterializedConnector systemConnector = new MaterializedConnector(systemId, new SystemConnector(
                nodeManager,
                systemTablesProvider,
                transactionId -> transactionManager.getConnectorTransaction(transactionId, catalogName)),
                () -> {});

        SecurityManagement securityManagement = connector.getAccessControl().isPresent() ? CONNECTOR : SYSTEM;

        Catalog catalog = new Catalog(
                catalogName.getCatalogName(),
                connector.getCatalogName(),
                connectorName,
                connector.getConnector(),
                securityManagement,
                informationSchemaConnector.getCatalogName(),
                informationSchemaConnector.getConnector(),
                systemConnector.getCatalogName(),
                systemConnector.getConnector());

        try {
            addConnectorInternal(connector);
            addConnectorInternal(informationSchemaConnector);
            addConnectorInternal(systemConnector);
            catalogManager.registerCatalog(catalog);
        }
        catch (Throwable e) {
            catalogManager.removeCatalog(catalog.getCatalogName());
            removeConnectorInternal(systemConnector.getCatalogName());
            removeConnectorInternal(informationSchemaConnector.getCatalogName());
            removeConnectorInternal(connector.getCatalogName());
            throw e;
        }

        connector.getEventListeners()
                .forEach(eventListenerManager::addEventListener);
    }

    private synchronized void addConnectorInternal(MaterializedConnector connector)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        CatalogName catalogName = connector.getCatalogName();
        checkState(!connectors.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        connectors.put(catalogName, connector);

        connector.getSplitManager()
                .ifPresent(connectorSplitManager -> splitManager.addConnectorSplitManager(catalogName, connectorSplitManager));

        connector.getPageSourceProvider()
                .ifPresent(pageSourceProvider -> pageSourceManager.addConnectorPageSourceProvider(catalogName, pageSourceProvider));

        connector.getPageSinkProvider()
                .ifPresent(pageSinkProvider -> pageSinkManager.addConnectorPageSinkProvider(catalogName, pageSinkProvider));

        connector.getIndexProvider()
                .ifPresent(indexProvider -> indexManager.addIndexProvider(catalogName, indexProvider));

        connector.getPartitioningProvider()
                .ifPresent(partitioningProvider -> nodePartitioningManager.addPartitioningProvider(catalogName, partitioningProvider));

        procedureRegistry.addProcedures(catalogName, connector.getProcedures());
        Set<TableProcedureMetadata> tableProcedures = connector.getTableProcedures();
        tableProceduresRegistry.addTableProcedures(catalogName, tableProcedures);

        connector.getAccessControl()
                .ifPresent(accessControl -> accessControlManager.addCatalogAccessControl(catalogName, accessControl));

        tablePropertyManager.addProperties(catalogName, connector.getTableProperties());
        materializedViewPropertyManager.addProperties(catalogName, connector.getMaterializedViewProperties());
        columnPropertyManager.addProperties(catalogName, connector.getColumnProperties());
        schemaPropertyManager.addProperties(catalogName, connector.getSchemaProperties());
        analyzePropertyManager.addProperties(catalogName, connector.getAnalyzeProperties());
        for (TableProcedureMetadata tableProcedure : tableProcedures) {
            tableProceduresPropertyManager.addProperties(catalogName, tableProcedure.getName(), tableProcedure.getProperties());
        }
        sessionPropertyManager.addConnectorSessionProperties(catalogName, connector.getSessionProperties());
    }

    public synchronized void dropConnection(String catalogName)
    {
        requireNonNull(catalogName, "catalogName is null");

        catalogManager.removeCatalog(catalogName).ifPresent(catalog -> {
            // todo wait for all running transactions using the connector to complete before removing the services
            removeConnectorInternal(catalog);
            removeConnectorInternal(createInformationSchemaCatalogName(catalog));
            removeConnectorInternal(createSystemTablesCatalogName(catalog));
        });
    }

    private synchronized void removeConnectorInternal(CatalogName catalogName)
    {
        splitManager.removeConnectorSplitManager(catalogName);
        pageSourceManager.removeConnectorPageSourceProvider(catalogName);
        pageSinkManager.removeConnectorPageSinkProvider(catalogName);
        indexManager.removeIndexProvider(catalogName);
        nodePartitioningManager.removePartitioningProvider(catalogName);
        procedureRegistry.removeProcedures(catalogName);
        tableProceduresRegistry.removeProcedures(catalogName);
        accessControlManager.removeCatalogAccessControl(catalogName);
        tablePropertyManager.removeProperties(catalogName);
        materializedViewPropertyManager.removeProperties(catalogName);
        columnPropertyManager.removeProperties(catalogName);
        schemaPropertyManager.removeProperties(catalogName);
        analyzePropertyManager.removeProperties(catalogName);
        tableProceduresPropertyManager.removeProperties(catalogName);
        sessionPropertyManager.removeConnectorSessionProperties(catalogName);

        MaterializedConnector materializedConnector = connectors.remove(catalogName);
        if (materializedConnector != null) {
            materializedConnector.shutdown();
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
                new InternalMetadataProvider(metadataManager, typeManager),
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

    private static class MaterializedConnector
    {
        private final CatalogName catalogName;
        private final Connector connector;
        private final Runnable afterShutdown;
        private final Set<SystemTable> systemTables;
        private final Set<Procedure> procedures;
        private final Set<TableProcedureMetadata> tableProcedures;
        private final Optional<ConnectorSplitManager> splitManager;
        private final Optional<ConnectorPageSourceProvider> pageSourceProvider;
        private final Optional<ConnectorPageSinkProvider> pageSinkProvider;
        private final Optional<ConnectorIndexProvider> indexProvider;
        private final Optional<ConnectorNodePartitioningProvider> partitioningProvider;
        private final Optional<ConnectorAccessControl> accessControl;
        private final List<EventListener> eventListeners;
        private final List<PropertyMetadata<?>> sessionProperties;
        private final List<PropertyMetadata<?>> tableProperties;
        private final List<PropertyMetadata<?>> materializedViewProperties;
        private final List<PropertyMetadata<?>> schemaProperties;
        private final List<PropertyMetadata<?>> columnProperties;
        private final List<PropertyMetadata<?>> analyzeProperties;

        public MaterializedConnector(CatalogName catalogName, Connector connector, Runnable afterShutdown)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.connector = requireNonNull(connector, "connector is null");
            this.afterShutdown = requireNonNull(afterShutdown, "afterShutdown is null");

            Set<SystemTable> systemTables = connector.getSystemTables();
            requireNonNull(systemTables, format("Connector '%s' returned a null system tables set", catalogName));
            this.systemTables = ImmutableSet.copyOf(systemTables);

            Set<Procedure> procedures = connector.getProcedures();
            requireNonNull(procedures, format("Connector '%s' returned a null procedures set", catalogName));
            this.procedures = ImmutableSet.copyOf(procedures);

            Set<TableProcedureMetadata> tableProcedures = connector.getTableProcedures();
            requireNonNull(procedures, format("Connector '%s' returned a null table procedures set", catalogName));
            this.tableProcedures = ImmutableSet.copyOf(tableProcedures);

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
            this.sessionProperties = ImmutableList.copyOf(sessionProperties);

            List<PropertyMetadata<?>> tableProperties = connector.getTableProperties();
            requireNonNull(tableProperties, format("Connector '%s' returned a null table properties set", catalogName));
            this.tableProperties = ImmutableList.copyOf(tableProperties);

            List<PropertyMetadata<?>> materializedViewProperties = connector.getMaterializedViewProperties();
            requireNonNull(materializedViewProperties, format("Connector '%s' returned a null materialized view properties set", catalogName));
            this.materializedViewProperties = ImmutableList.copyOf(materializedViewProperties);

            List<PropertyMetadata<?>> schemaProperties = connector.getSchemaProperties();
            requireNonNull(schemaProperties, format("Connector '%s' returned a null schema properties set", catalogName));
            this.schemaProperties = ImmutableList.copyOf(schemaProperties);

            List<PropertyMetadata<?>> columnProperties = connector.getColumnProperties();
            requireNonNull(columnProperties, format("Connector '%s' returned a null column properties set", catalogName));
            this.columnProperties = ImmutableList.copyOf(columnProperties);

            List<PropertyMetadata<?>> analyzeProperties = connector.getAnalyzeProperties();
            requireNonNull(analyzeProperties, format("Connector '%s' returned a null analyze properties set", catalogName));
            this.analyzeProperties = ImmutableList.copyOf(analyzeProperties);
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

        public Set<Procedure> getProcedures()
        {
            return procedures;
        }

        public Set<TableProcedureMetadata> getTableProcedures()
        {
            return tableProcedures;
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

        public Optional<ConnectorAccessControl> getAccessControl()
        {
            return accessControl;
        }

        public List<EventListener> getEventListeners()
        {
            return eventListeners;
        }

        public List<PropertyMetadata<?>> getSessionProperties()
        {
            return sessionProperties;
        }

        public List<PropertyMetadata<?>> getTableProperties()
        {
            return tableProperties;
        }

        public List<PropertyMetadata<?>> getMaterializedViewProperties()
        {
            return materializedViewProperties;
        }

        public List<PropertyMetadata<?>> getColumnProperties()
        {
            return columnProperties;
        }

        public List<PropertyMetadata<?>> getSchemaProperties()
        {
            return schemaProperties;
        }

        public List<PropertyMetadata<?>> getAnalyzeProperties()
        {
            return analyzeProperties;
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
    }
}
