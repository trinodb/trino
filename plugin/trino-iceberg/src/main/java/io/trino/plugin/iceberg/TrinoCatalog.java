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

import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TrinoCatalog
{
    String getName(ConnectorSession session);

    List<Namespace> listNamespaces(ConnectorSession session);

    boolean dropNamespace(Namespace namespace, ConnectorSession session) throws NamespaceNotEmptyException;

    Map<String, Object> loadNamespaceMetadataObjects(Namespace namespace, ConnectorSession session) throws NoSuchNamespaceException;

    TrinoPrincipal getNamespacePrincipal(Namespace namespace, ConnectorSession session) throws NoSuchNamespaceException;

    void createNamespaceWithPrincipal(Namespace namespace, Map<String, Object> map, TrinoPrincipal owner, ConnectorSession session);

    void setNamespacePrincipal(Namespace namespace, TrinoPrincipal principal, ConnectorSession session) throws NoSuchNamespaceException;

    void renameNamespace(Namespace source, Namespace target, ConnectorSession session) throws NoSuchNamespaceException;

    List<TableIdentifier> listTables(Namespace namespace, ConnectorSession session);

    Transaction newCreateTableTransaction(TableIdentifier tableIdentifier, Schema schema, PartitionSpec partitionSpec, String location,
            Map<String, String> properties, ConnectorSession session);

    boolean tableExists(TableIdentifier identifier, ConnectorSession session);

    boolean dropTable(TableIdentifier identifier, boolean purge, ConnectorSession session);

    void renameTable(TableIdentifier from, TableIdentifier to, ConnectorSession session);

    Table loadTable(TableIdentifier identifier, ConnectorSession session);

    void updateTableComment(TableIdentifier tableIdentifier, Optional<String> comment, ConnectorSession session);

    String defaultTableLocation(TableIdentifier tableIdentifier, ConnectorSession session);

    void createMaterializedView(TableIdentifier viewIdentifier, ConnectorMaterializedViewDefinition definition,
            boolean replace, boolean ignoreExisting, ConnectorSession session);

    void dropMaterializedView(TableIdentifier viewIdentifier, ConnectorSession session);

    Optional<ConnectorMaterializedViewDefinition> getMaterializedView(TableIdentifier viewIdentifier, ConnectorSession session);

    List<TableIdentifier> listViews(Namespace namespace, ConnectorSession session);

    List<TableIdentifier> listMaterializedViews(Namespace namespace, ConnectorSession session);
}
