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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.getSchemaLocation;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class IcebergHadoopMetadata
        extends IcebergMetadata
{
    private final HdfsEnvironment hdfsEnvironment;

    public IcebergHadoopMetadata(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec)
    {
        super(hdfsEnvironment, typeManager, commitTaskCodec);
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableIdentifier tableIdentifier = TableIdentifier.of(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        try {
            HadoopCatalog hadoopCatalog = getHadoopCatalog(session);
            return hadoopCatalog.loadTable(tableIdentifier);
        }
        catch (NoSuchTableException e) {
            throw new TableNotFoundException(schemaTableName);
        }
    }

    private HadoopCatalog getHadoopCatalog(ConnectorSession session)
    {
        HdfsContext hdfsContext = new HdfsContext(session, "not used");
        Path path = new Path("/");
        Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, path);
        String warehouse = configuration.get("hive.metastore.warehouse.dir");
        if (warehouse == null || warehouse.isEmpty()) {
            throw new TrinoException(HIVE_METASTORE_ERROR, format("hive.metastore.warehouse.dir not set"));
        }
        return getHadoopCatalog(session, warehouse);
    }

    /**
     * Get catalog a given location.
     * @param session
     * @param location
     * @return
     */
    private HadoopCatalog getHadoopCatalog(ConnectorSession session, String location)
    {
        HdfsContext hdfsContext = new HdfsContext(session, "not used");
        Path path = new Path(location);
        Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, path);
        return new HadoopCatalog(configuration, location);
    }

    private boolean icebergSchemaExists(ConnectorSession session, String schemaName)
    {
        try {
            HadoopCatalog hadoopCatalog = getHadoopCatalog(session);
            Namespace namespace = Namespace.of(schemaName);
            hadoopCatalog.loadNamespaceMetadata(namespace);
            return true;
        }
        catch (NoSuchNamespaceException e) {
            return false;
        }
    }

    private boolean icebergTableExists(ConnectorSession session, SchemaTableName table)
    {
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(table.getSchemaName(), table.getTableName());
            HadoopCatalog hadoopCatalog = getHadoopCatalog(session);
            hadoopCatalog.loadTable(tableIdentifier);
            return true;
        }
        catch (NoSuchTableException e) {
            return false;
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        HadoopCatalog hadoopCatalog = getHadoopCatalog(session);
        try {
            List<Namespace> namespaces = hadoopCatalog.listNamespaces(Namespace.empty());
            return namespaces.stream()
                    .map(namespace -> namespace.toString())
                    .collect(toList());
        }
        catch (NoSuchNamespaceException e) {
            return ImmutableList.of();
        }
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        HadoopCatalog hadoopCatalog = getHadoopCatalog(session);
        Namespace namespace = Namespace.of(schemaName.getSchemaName());
        Map<String, String> namespaceMetadata = hadoopCatalog.loadNamespaceMetadata(namespace);
        return namespaceMetadata.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> e.getValue()));
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, CatalogSchemaName schemaName)
    {
        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        HadoopCatalog hadoopCatalog = getHadoopCatalog(session);
        List<Namespace> namespaces = hadoopCatalog.listNamespaces(Namespace.empty());
        return namespaces.stream()
                .flatMap(namespace -> hadoopCatalog.listTables(namespace).stream()
                        .map(tableIdentifier -> new SchemaTableName(tableIdentifier.namespace().toString(), tableIdentifier.name()))
                        .collect(toList())
                        .stream())
                .collect(toList());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        Optional<String> location = getSchemaLocation(properties).map(uri -> {
            try {
                hdfsEnvironment.getFileSystem(new HdfsContext(session, schemaName), new Path(uri));
            }
            catch (IOException | IllegalArgumentException e) {
                throw new TrinoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + uri, e);
            }
            return uri;
        });

        if (location.isEmpty()) {
            HadoopCatalog hadoopCatalog = getHadoopCatalog(session);
            hadoopCatalog.createNamespace(Namespace.of(schemaName));
        }
        else {
            HadoopCatalog hadoopCatalog = getHadoopCatalog(session, location.get());
            hadoopCatalog.createNamespace(Namespace.of(schemaName));
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        HadoopCatalog hadoopCatalog = getHadoopCatalog(session);
        hadoopCatalog.dropNamespace(Namespace.of(schemaName));
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String source, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());
        if (comment.isEmpty()) {
            icebergTable.updateProperties().remove(TABLE_COMMENT).commit();
        }
        else {
            icebergTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
        }
    }

    @Override
    protected Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, String targetPath, ImmutableMap<String, String> newTableProperties)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        if (!icebergSchemaExists(session, schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }
        if (icebergTableExists(session, schemaTableName)) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        HdfsContext hdfsContext = new HdfsContext(session, schemaName, tableName);
        HadoopCatalog catalog;
        if (targetPath == null) {
            catalog = getHadoopCatalog(session);
        }
        else {
            catalog = getHadoopCatalog(session, targetPath);
        }

        TableIdentifier tableIdentifier = TableIdentifier.of(schemaName, tableName);
        return catalog.newCreateTableTransaction(tableIdentifier, schema, partitionSpec, newTableProperties);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        TableIdentifier tableIdentifier = TableIdentifier.of(handle.getSchemaName(), handle.getTableName());
        getHadoopCatalog(session).dropTable(tableIdentifier, false);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        TableIdentifier tableIdentifier = TableIdentifier.of(handle.getSchemaName(), handle.getTableName());
        TableIdentifier newTableIdentifier = TableIdentifier.of(newTable.getSchemaName(), newTable.getTableName());
        getHadoopCatalog(session).renameTable(tableIdentifier, newTableIdentifier);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
    }

    @Override
    protected boolean isMaterializedView(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return false;
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }
}
