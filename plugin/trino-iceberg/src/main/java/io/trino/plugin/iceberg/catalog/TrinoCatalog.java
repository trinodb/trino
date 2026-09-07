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
package io.trino.plugin.iceberg.catalog;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.TableInfo;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.UnknownTableTypeException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.security.TrinoPrincipal;
import jakarta.annotation.Nullable;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog.DEPENDS_ON_NON_DETERMINISTIC_FUNCTIONS;
import static io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog.DEPENDS_ON_TABLES;
import static io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog.DEPENDS_ON_TABLE_FUNCTIONS;
import static io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog.TRINO_QUERY_START_TIME;
import static io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog.UNKNOWN_SNAPSHOT_TOKEN;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

/**
 * An interface to allow different Iceberg catalog implementations in IcebergMetadata.
 * <p>
 * It mimics the Iceberg catalog interface, with the following modifications:
 * <ul>
 *   <li>ConnectorSession is added at the front of each method signature</li>
 *   <li>String is used to identify namespace instead of Iceberg Namespace, Optional.empty() is used to represent Namespace.empty().
 *      This delegates the handling of multi-level namespace to each implementation</li>
 *   <li>Similarly, SchemaTableName is used to identify table instead of Iceberg TableIdentifier</li>
 *   <li>Metadata is a map of string to object instead of string to string</li>
 *   <li>Additional methods related to authorization are added</li>
 *   <li>View related methods are currently mostly the same as ones in ConnectorMetadata.
 *      These methods will likely be updated once Iceberg view interface is added.</li>
 * </ul>
 */
public interface TrinoCatalog
{
    boolean namespaceExists(ConnectorSession session, String namespace);

    List<String> listNamespaces(ConnectorSession session);

    void dropNamespace(ConnectorSession session, String namespace);

    default Optional<String> getNamespaceSeparator()
    {
        return Optional.empty();
    }

    Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace);

    Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace);

    void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner);

    void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal);

    void renameNamespace(ConnectorSession session, String source, String target);

    List<TableInfo> listTables(ConnectorSession session, Optional<String> namespace);

    List<SchemaTableName> listIcebergTables(ConnectorSession session, List<String> filter);

    default List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return listTables(session, namespace).stream()
                .filter(info -> info.extendedRelationType() == TableInfo.ExtendedRelationType.TRINO_VIEW)
                .map(TableInfo::tableName)
                .collect(toImmutableList());
    }

    default List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        return listTables(session, namespace).stream()
                .filter(info -> info.extendedRelationType() == TableInfo.ExtendedRelationType.TRINO_MATERIALIZED_VIEW)
                .map(TableInfo::tableName)
                .collect(toImmutableList());
    }

    default Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableMap.Builder<SchemaTableName, RelationType> result = ImmutableMap.builder();
        for (TableInfo info : listTables(session, namespace)) {
            result.put(info.tableName(), info.extendedRelationType().toRelationType());
        }
        return result.buildKeepingLast();
    }

    Optional<Iterator<RelationColumnsMetadata>> streamRelationColumns(
            ConnectorSession session,
            Optional<String> namespace,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Predicate<SchemaTableName> isRedirected);

    Optional<Iterator<RelationCommentMetadata>> streamRelationComments(
            ConnectorSession session,
            Optional<String> namespace,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Predicate<SchemaTableName> isRedirected);

    default Transaction newTransaction(Table icebergTable)
    {
        return icebergTable.newTransaction();
    }

    Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            Optional<String> location,
            Map<String, String> properties);

    Transaction newCreateOrReplaceTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            String location,
            Map<String, String> properties);

    void registerTable(ConnectorSession session, SchemaTableName tableName, TableMetadata tableMetadata);

    void unregisterTable(ConnectorSession session, SchemaTableName tableName);

    void dropTable(ConnectorSession session, SchemaTableName schemaTableName);

    void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName);

    void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to);

    /**
     * load an Iceberg table
     *
     * @param session Trino session
     * @param schemaTableName Trino schema and table name
     * @return Iceberg table loaded
     * @throws UnknownTableTypeException if table is not of Iceberg type in the metastore
     */
    BaseTable loadTable(ConnectorSession session, SchemaTableName schemaTableName);

    /**
     * Bulk load column metadata. The returned map may contain fewer entries then asked for.
     */
    Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables);

    void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment);

    void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment);

    void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment);

    @Nullable
    String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName);

    void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal);

    void createView(
            ConnectorSession session,
            SchemaTableName schemaViewName,
            ConnectorViewDefinition definition,
            Map<String, Object> viewProperties,
            boolean replace);

    void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target);

    void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal);

    void dropView(ConnectorSession session, SchemaTableName schemaViewName);

    Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace);

    Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName);

    default Map<String, Object> getViewProperties(ConnectorSession session, SchemaTableName viewName)
    {
        return ImmutableMap.of();
    }

    void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties,
            boolean replace,
            boolean ignoreExisting);

    void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment);

    void dropMaterializedView(ConnectorSession session, SchemaTableName viewName);

    Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName);

    Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition);

    Optional<BaseTable> getMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName);

    void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target);

    default MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName materializedViewName, boolean considerGracePeriod)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support materialized views");
    }

    default void recordMaterializedViewRefresh(
            ConnectorSession session,
            SchemaTableName materializedViewName,
            AppendFiles appendFiles,
            List<ConnectorTableHandle> sourceTableHandles,
            List<CatalogSchemaTableName> sourceViewNames,
            boolean hasForeignSourceTables,
            boolean hasSourceTableFunctions,
            boolean hasNonDeterministicFunctions)
    {
        List<String> tableDependencies = new ArrayList<>();
        sourceTableHandles.stream()
                .map(IcebergTableHandle.class::cast)
                .map(handle -> "%s=%s".formatted(
                        handle.getSchemaTableName(),
                        handle.getSnapshotId().isPresent() ? Long.toString(handle.getSnapshotId().orElseThrow()) : ""))
                .forEach(tableDependencies::add);
        if (hasForeignSourceTables) {
            tableDependencies.add(UNKNOWN_SNAPSHOT_TOKEN);
        }

        // Update the 'dependsOnTables' property that tracks tables on which the materialized view depends and the corresponding snapshot ids of the tables
        appendFiles.set(DEPENDS_ON_TABLES, String.join(",", tableDependencies));
        appendFiles.set(DEPENDS_ON_TABLE_FUNCTIONS, String.valueOf(hasSourceTableFunctions));
        appendFiles.set(DEPENDS_ON_NON_DETERMINISTIC_FUNCTIONS, String.valueOf(hasNonDeterministicFunctions));
        appendFiles.set(TRINO_QUERY_START_TIME, session.getStart().toString());
    }

    default OptionalLong getMaterializedViewIncrementalRefreshFromSnapshot(Table storageTable, List<ConnectorTableHandle> sourceTableHandles)
    {
        if (sourceTableHandles.size() != 1) {
            return OptionalLong.empty();
        }

        Optional<String> dependencies = Optional.ofNullable(storageTable.currentSnapshot())
                .map(Snapshot::summary)
                .map(summary -> summary.get(DEPENDS_ON_TABLES));
        if (dependencies.isEmpty() || dependencies.get().equals(UNKNOWN_SNAPSHOT_TOKEN)) {
            return OptionalLong.empty();
        }

        Map<String, String> sourceTableToSnapshot = Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(dependencies.get());
        if (sourceTableToSnapshot.size() != 1) {
            return OptionalLong.empty();
        }
        Entry<String, String> sourceTable = getOnlyElement(sourceTableToSnapshot.entrySet());
        String[] schemaTable = sourceTable.getKey().split("\\.");
        IcebergTableHandle handle = (IcebergTableHandle) getOnlyElement(sourceTableHandles);
        SchemaTableName sourceSchemaTable = new SchemaTableName(schemaTable[0], schemaTable[1]);
        if (!sourceSchemaTable.equals(handle.getSchemaTableName())) {
            return OptionalLong.empty();
        }

        return OptionalLong.of(Long.parseLong(sourceTable.getValue()));
    }

    void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment);

    Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String hiveCatalogName);

    default Metrics getMetrics()
    {
        return Metrics.EMPTY;
    }
}
