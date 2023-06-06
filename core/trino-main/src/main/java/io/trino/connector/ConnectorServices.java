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
import io.opentelemetry.api.trace.Tracer;
import io.trino.metadata.CatalogMetadata.SecurityManagement;
import io.trino.metadata.CatalogProcedures;
import io.trino.metadata.CatalogTableFunctions;
import io.trino.metadata.CatalogTableProcedures;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorIndexProvider;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ArgumentSpecification;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable;
import io.trino.spi.function.table.TableArgumentSpecification;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.split.RecordPageSourceProvider;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.metadata.CatalogMetadata.SecurityManagement.CONNECTOR;
import static io.trino.metadata.CatalogMetadata.SecurityManagement.SYSTEM;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConnectorServices
{
    private static final Logger log = Logger.get(ConnectorServices.class);

    private final Tracer tracer;
    private final CatalogHandle catalogHandle;
    private final Connector connector;
    private final Runnable afterShutdown;
    private final Set<SystemTable> systemTables;
    private final CatalogProcedures procedures;
    private final CatalogTableProcedures tableProcedures;
    private final Optional<FunctionProvider> functionProvider;
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

    private final AtomicBoolean shutdown = new AtomicBoolean();

    public ConnectorServices(Tracer tracer, CatalogHandle catalogHandle, Connector connector, Runnable afterShutdown)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.connector = requireNonNull(connector, "connector is null");
        this.afterShutdown = requireNonNull(afterShutdown, "afterShutdown is null");

        Set<SystemTable> systemTables = connector.getSystemTables();
        requireNonNull(systemTables, format("Connector '%s' returned a null system tables set", catalogHandle));
        this.systemTables = ImmutableSet.copyOf(systemTables);

        Set<Procedure> procedures = connector.getProcedures();
        requireNonNull(procedures, format("Connector '%s' returned a null procedures set", catalogHandle));
        this.procedures = new CatalogProcedures(procedures);

        Set<TableProcedureMetadata> tableProcedures = connector.getTableProcedures();
        requireNonNull(procedures, format("Connector '%s' returned a null table procedures set", catalogHandle));
        this.tableProcedures = new CatalogTableProcedures(tableProcedures);

        this.functionProvider = requireNonNull(connector.getFunctionProvider(), format("Connector '%s' returned a null function provider", catalogHandle));

        Set<ConnectorTableFunction> tableFunctions = connector.getTableFunctions();
        requireNonNull(tableFunctions, format("Connector '%s' returned a null table functions set", catalogHandle));
        this.tableFunctions = new CatalogTableFunctions(tableFunctions);
        // TODO ConnectorTableFunction should be converted to a metadata class (and a separate analysis interface) which performs this validation in the constructor
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
            requireNonNull(connectorPageSourceProvider, format("Connector '%s' returned a null page source provider", catalogHandle));
        }
        catch (UnsupportedOperationException ignored) {
        }

        try {
            ConnectorRecordSetProvider connectorRecordSetProvider = connector.getRecordSetProvider();
            requireNonNull(connectorRecordSetProvider, format("Connector '%s' returned a null record set provider", catalogHandle));
            verify(connectorPageSourceProvider == null, "Connector '%s' returned both page source and record set providers", catalogHandle);
            connectorPageSourceProvider = new RecordPageSourceProvider(connectorRecordSetProvider);
        }
        catch (UnsupportedOperationException ignored) {
        }
        this.pageSourceProvider = Optional.ofNullable(connectorPageSourceProvider);

        ConnectorPageSinkProvider connectorPageSinkProvider = null;
        try {
            connectorPageSinkProvider = connector.getPageSinkProvider();
            requireNonNull(connectorPageSinkProvider, format("Connector '%s' returned a null page sink provider", catalogHandle));
        }
        catch (UnsupportedOperationException ignored) {
        }
        this.pageSinkProvider = Optional.ofNullable(connectorPageSinkProvider);

        ConnectorIndexProvider indexProvider = null;
        try {
            indexProvider = connector.getIndexProvider();
            requireNonNull(indexProvider, format("Connector '%s' returned a null index provider", catalogHandle));
        }
        catch (UnsupportedOperationException ignored) {
        }
        this.indexProvider = Optional.ofNullable(indexProvider);

        ConnectorNodePartitioningProvider partitioningProvider = null;
        try {
            partitioningProvider = connector.getNodePartitioningProvider();
            requireNonNull(partitioningProvider, format("Connector '%s' returned a null partitioning provider", catalogHandle));
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
        requireNonNull(sessionProperties, format("Connector '%s' returned a null system properties set", catalogHandle));
        this.sessionProperties = Maps.uniqueIndex(sessionProperties, PropertyMetadata::getName);

        List<PropertyMetadata<?>> tableProperties = connector.getTableProperties();
        requireNonNull(tableProperties, format("Connector '%s' returned a null table properties set", catalogHandle));
        this.tableProperties = Maps.uniqueIndex(tableProperties, PropertyMetadata::getName);

        List<PropertyMetadata<?>> materializedViewProperties = connector.getMaterializedViewProperties();
        requireNonNull(materializedViewProperties, format("Connector '%s' returned a null materialized view properties set", catalogHandle));
        this.materializedViewProperties = Maps.uniqueIndex(materializedViewProperties, PropertyMetadata::getName);

        List<PropertyMetadata<?>> schemaProperties = connector.getSchemaProperties();
        requireNonNull(schemaProperties, format("Connector '%s' returned a null schema properties set", catalogHandle));
        this.schemaProperties = Maps.uniqueIndex(schemaProperties, PropertyMetadata::getName);

        List<PropertyMetadata<?>> columnProperties = connector.getColumnProperties();
        requireNonNull(columnProperties, format("Connector '%s' returned a null column properties set", catalogHandle));
        this.columnProperties = Maps.uniqueIndex(columnProperties, PropertyMetadata::getName);

        List<PropertyMetadata<?>> analyzeProperties = connector.getAnalyzeProperties();
        requireNonNull(analyzeProperties, format("Connector '%s' returned a null analyze properties set", catalogHandle));
        this.analyzeProperties = Maps.uniqueIndex(analyzeProperties, PropertyMetadata::getName);

        Set<ConnectorCapabilities> capabilities = connector.getCapabilities();
        requireNonNull(capabilities, format("Connector '%s' returned a null capabilities set", catalogHandle));
        this.capabilities = capabilities;
    }

    public Tracer getTracer()
    {
        return tracer;
    }

    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
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

    public FunctionProvider getFunctionProvider()
    {
        checkArgument(functionProvider.isPresent(), "Connector '%s' does not have functions", catalogHandle);
        return functionProvider.get();
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
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connector.getClass().getClassLoader())) {
            connector.shutdown();
        }
        catch (Throwable t) {
            log.error(t, "Error shutting down catalog: %s", catalogHandle);
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
        // The 'keep when empty' or 'prune when empty' property must not be explicitly specified for a table argument with row semantics.
        // Such a table argument is implicitly 'prune when empty'. The TableArgumentSpecification.Builder enforces the 'prune when empty' property
        // for a table argument with row semantics.

        if (tableFunction.getReturnTypeSpecification() instanceof DescribedTable describedTable) {
            checkArgument(describedTable.getDescriptor().isTyped(), "field types missing in returned type specification");
        }
    }
}
