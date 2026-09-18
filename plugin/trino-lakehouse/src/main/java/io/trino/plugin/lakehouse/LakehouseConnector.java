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
package io.trino.plugin.lakehouse;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveSchemaProperties;
import io.trino.plugin.iceberg.IcebergMaterializedViewProperties;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.immutableEnumSet;
import static io.trino.spi.connector.ConnectorCapabilities.MATERIALIZED_VIEW_GRACE_PERIOD;
import static io.trino.spi.connector.ConnectorCapabilities.MATERIALIZED_VIEW_WHEN_STALE_BEHAVIOR;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class LakehouseConnector
        implements Connector
{
    private static final Logger log = Logger.get(LakehouseConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final LakehouseTransactionManager transactionManager;
    private final LakehouseSplitManager splitManager;
    private final LakehousePageSourceProviderFactory pageSourceProviderFactory;
    private final LakehousePageSinkProvider pageSinkProvider;
    private final LakehouseNodePartitioningProvider nodePartitioningProvider;
    private final LakehouseSessionProperties sessionProperties;
    private final LakehouseTableProperties tableProperties;
    private final IcebergMaterializedViewProperties materializedViewProperties;
    private final Set<Procedure> procedures;
    private final Set<TableProcedureMetadata> tableProcedures;

    @Inject
    public LakehouseConnector(
            LifeCycleManager lifeCycleManager,
            LakehouseTransactionManager transactionManager,
            LakehouseSplitManager splitManager,
            LakehousePageSourceProviderFactory pageSourceProviderFactory,
            LakehousePageSinkProvider pageSinkProvider,
            LakehouseNodePartitioningProvider nodePartitioningProvider,
            LakehouseSessionProperties sessionProperties,
            LakehouseTableProperties tableProperties,
            IcebergMaterializedViewProperties materializedViewProperties,
            Map<TableType, Set<Procedure>> procedures,
            Map<TableType, Set<TableProcedureMetadata>> tableProcedures)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProviderFactory = requireNonNull(pageSourceProviderFactory, "pageSourceProviderFactory is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.materializedViewProperties = requireNonNull(materializedViewProperties, "materializedViewProperties is null");
        this.procedures = mergeProcedures(procedures);
        this.tableProcedures = mergeTableProcedures(tableProcedures);
    }

    /**
     * Merges the procedures of every table type. A procedure carries the implementation of the
     * table type declaring it, so a name declared by more than one table type cannot be dispatched
     * and is not exposed.
     */
    private static Set<Procedure> mergeProcedures(Map<TableType, Set<Procedure>> proceduresByTableType)
    {
        ImmutableListMultimap.Builder<String, TableType> builder = ImmutableListMultimap.builder();
        proceduresByTableType.forEach((tableType, procedures) -> procedures.forEach(procedure -> builder.put(procedure.getName(), tableType)));
        ListMultimap<String, TableType> declaringTableTypes = builder.build();

        declaringTableTypes.asMap().forEach((name, tableTypes) -> {
            if (tableTypes.size() > 1) {
                log.warn("Not exposing procedure %s, it is declared by %s", name, tableTypes);
            }
        });

        return proceduresByTableType.values().stream()
                .flatMap(Set::stream)
                .filter(procedure -> declaringTableTypes.get(procedure.getName()).size() == 1)
                .collect(toImmutableSet());
    }

    /**
     * Merges the table procedures of every table type. A table procedure carries no implementation,
     * so a name declared by more than one table type is exposed once and dispatched by
     * {@link LakehouseMetadata}. Every declaration must use the same metadata because the engine
     * validates the statement before the table type is known.
     */
    @VisibleForTesting
    static Set<TableProcedureMetadata> mergeTableProcedures(Map<TableType, Set<TableProcedureMetadata>> tableProceduresByTableType)
    {
        Map<String, TableProcedureMetadata> tableProcedures = new LinkedHashMap<>();
        // the declaration of the first table type in enum order is the one exposed, the rest only have to match it
        for (TableType tableType : TableType.values()) {
            for (TableProcedureMetadata procedure : tableProceduresByTableType.getOrDefault(tableType, ImmutableSet.of())) {
                TableProcedureMetadata existing = tableProcedures.putIfAbsent(procedure.getName(), procedure);
                if (existing != null) {
                    verifySameMetadata(existing, procedure);
                }
            }
        }
        return ImmutableSet.copyOf(tableProcedures.values());
    }

    private static void verifySameMetadata(TableProcedureMetadata left, TableProcedureMetadata right)
    {
        verify(left.getExecutionMode().isReadsData() == right.getExecutionMode().isReadsData(),
                "Table procedure %s declares different reads data execution mode",
                left.getName());
        verify(left.getExecutionMode().supportsFilter() == right.getExecutionMode().supportsFilter(),
                "Table procedure %s declares different supports filter execution mode",
                left.getName());
        verify(properties(left).equals(properties(right)),
                "Table procedure %s declares different properties: %s and %s",
                left.getName(),
                properties(left),
                properties(right));
    }

    private record ProcedureProperty(String name, Type sqlType, Object defaultValue) {}

    /**
     * Describes a property by everything the engine uses to validate a statement before the table type is known.
     */
    private static Set<ProcedureProperty> properties(TableProcedureMetadata procedure)
    {
        return procedure.getProperties().stream()
                .map(property -> new ProcedureProperty(property.getName(), property.getSqlType(), property.getDefaultValue()))
                .collect(toImmutableSet());
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
        return transactionManager.begin();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return transactionManager.get(transactionHandle, session.getIdentity());
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProviderFactory getPageSourceProviderFactory()
    {
        return pageSourceProviderFactory;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return nodePartitioningProvider;
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        transactionManager.commit(transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        transactionManager.rollback(transactionHandle);
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public Set<TableProcedureMetadata> getTableProcedures()
    {
        return tableProcedures;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return HiveSchemaProperties.SCHEMA_PROPERTIES;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties.getTableProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return materializedViewProperties.getMaterializedViewProperties();
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return immutableEnumSet(NOT_NULL_COLUMN_CONSTRAINT, MATERIALIZED_VIEW_GRACE_PERIOD, MATERIALIZED_VIEW_WHEN_STALE_BEHAVIOR);
    }
}
