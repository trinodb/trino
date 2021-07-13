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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.util.PropertyUtil;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.trino.plugin.iceberg.IcebergTableProperties.getTableLocation;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromTableId;
import static io.trino.plugin.iceberg.IcebergUtil.toIcebergSchema;
import static io.trino.plugin.iceberg.IcebergUtil.toTableId;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;

public interface TrinoCatalog
{
    void createNamespace(Namespace namespace, Map<String, String> metadata, ConnectorSession session);

    List<Namespace> listNamespaces(ConnectorSession session);

    Map<String, String> loadNamespaceMetadata(Namespace namespace, ConnectorSession session) throws NoSuchNamespaceException;

    boolean dropNamespace(Namespace namespace, ConnectorSession session) throws NamespaceNotEmptyException;

    default Map<String, Object> loadNamespaceMetadataObjects(Namespace namespace, ConnectorSession session) throws NoSuchNamespaceException
    {
        return loadNamespaceMetadata(namespace, session)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    default TrinoPrincipal getNamespacePrincipal(Namespace namespace, ConnectorSession session) throws NoSuchNamespaceException
    {
        throw new TrinoException(NOT_SUPPORTED, "getNamespacePrincipal is not supported by " + name(session));
    }

    default void createNamespaceWithPrincipal(Namespace namespace, Map<String, Object> map, TrinoPrincipal owner, ConnectorSession session)
    {
        Map<String, String> strMap = map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        createNamespace(namespace, strMap, session);
    }

    default void setNamespacePrincipal(Namespace namespace, TrinoPrincipal principal, ConnectorSession session) throws NoSuchNamespaceException
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported by " + name(session));
    }

    default void renameNamespace(Namespace source, Namespace target, ConnectorSession session) throws NoSuchNamespaceException
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported by " + name(session));
    }

    String name(ConnectorSession session);

    List<TableIdentifier> listTables(Namespace namespace, ConnectorSession session);

    Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties, ConnectorSession session);

    Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, Map<String, String> properties, ConnectorSession session);

    Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, ConnectorSession session);

    Table createTable(TableIdentifier identifier, Schema schema, ConnectorSession session);


    boolean tableExists(TableIdentifier identifier, ConnectorSession session);

    boolean dropTable(TableIdentifier identifier, ConnectorSession session);

    boolean dropTable(TableIdentifier identifier, boolean purge, ConnectorSession session);

    void renameTable(TableIdentifier from, TableIdentifier to, ConnectorSession session);

    Table loadTable(TableIdentifier identifier, ConnectorSession session);

    Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties, ConnectorSession session);

    default Transaction newCreateTableTransaction(ConnectorTableMetadata tableMetadata, ConnectorSession session)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        TableIdentifier tableId = toTableId(schemaTableName);

        if (tableExists(tableId, session)) {
            throw new AlreadyExistsException("Table already exists: %s", tableId);
        }

        Schema schema = toIcebergSchema(tableMetadata.getColumns());

        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));

        String targetPath = getTableLocation(tableMetadata.getProperties());
        if (targetPath == null) {
            targetPath = tableDefaultLocation(tableId, session);
        }

        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builderWithExpectedSize(2);
        FileFormat fileFormat = IcebergTableProperties.getFileFormat(tableMetadata.getProperties());
        propertiesBuilder.put(DEFAULT_FILE_FORMAT, fileFormat.toString());
        if (tableMetadata.getComment().isPresent()) {
            propertiesBuilder.put(TABLE_COMMENT, tableMetadata.getComment().get());
        }

        return newCreateTableTransaction(tableId, schema, partitionSpec, targetPath, propertiesBuilder.build(), session);
    }

    default void updateTableComment(TableIdentifier tableIdentifier, Optional<String> comment, ConnectorSession session)
    {
        UpdateProperties update = loadTable(tableIdentifier, session).updateProperties();
        comment.ifPresentOrElse(c -> update.set(TABLE_COMMENT, c), () -> update.remove(TABLE_COMMENT));
    }

    default String warehouseLocation(ConnectorSession session)
    {
        throw new TrinoException(NOT_SUPPORTED, "warehouseLocation is not supported");
    }

    default String tableDefaultLocation(TableIdentifier tableIdentifier, ConnectorSession session)
    {
        String dbLocationUri = PropertyUtil.propertyAsString(loadNamespaceMetadata(tableIdentifier.namespace(), session), "locationUri", null);
        if (dbLocationUri != null) {
            return String.format("%s/%s", dbLocationUri, tableIdentifier.name());
        }

        return String.format(
                "%s/%s.db/%s",
                warehouseLocation(session),
                schemaFromTableId(tableIdentifier),
                tableIdentifier.name());
    }

    default void createMaterializedView(TableIdentifier viewIdentifier, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting, ConnectorSession session)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported by " + name(session));
    }

    default void dropMaterializedView(TableIdentifier viewIdentifier, ConnectorSession session)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported" + name(session));
    }

    default Optional<ConnectorMaterializedViewDefinition> getMaterializedView(TableIdentifier viewIdentifier, ConnectorSession session)
    {
        throw new TrinoException(NOT_SUPPORTED, "getMaterializedView is not supported" + name(session));
    }

    default List<TableIdentifier> listViews(Namespace namespace, ConnectorSession session)
    {
        throw new TrinoException(NOT_SUPPORTED, "listViews is not supported" + name(session));
    }

    default List<TableIdentifier> listMaterializedViews(Namespace namespace, ConnectorSession session)
    {
        throw new TrinoException(NOT_SUPPORTED, "listMaterializedViews is not supported" + name(session));
    }
}
