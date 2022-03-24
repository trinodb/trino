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

import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.UnknownTableTypeException;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    List<String> listNamespaces(ConnectorSession session);

    void dropNamespace(ConnectorSession session, String namespace);

    Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace);

    Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace);

    void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner);

    void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal);

    void renameNamespace(ConnectorSession session, String source, String target);

    List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace);

    Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            String location,
            Map<String, String> properties);

    void dropTable(ConnectorSession session, SchemaTableName schemaTableName);

    void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to);

    /**
     * load an Iceberg table
     * @param session Trino session
     * @param schemaTableName Trino schema and table name
     * @return Iceberg table loaded
     * @throws UnknownTableTypeException if table is not of Iceberg type in the metastore
     */
    Table loadTable(ConnectorSession session, SchemaTableName schemaTableName);

    void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment);

    String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName);

    void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal);

    void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace);

    void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target);

    void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal);

    void dropView(ConnectorSession session, SchemaTableName schemaViewName);

    List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace);

    Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace);

    Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName);

    List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace);

    void createMaterializedView(
            ConnectorSession session,
            SchemaTableName schemaViewName,
            ConnectorMaterializedViewDefinition definition,
            boolean replace,
            boolean ignoreExisting);

    void dropMaterializedView(ConnectorSession session, SchemaTableName schemaViewName);

    Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName);

    void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target);

    void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment);
}
