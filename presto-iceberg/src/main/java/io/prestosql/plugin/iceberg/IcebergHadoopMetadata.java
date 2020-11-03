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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.TableAlreadyExistsException;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.prestosql.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.prestosql.plugin.iceberg.IcebergSchemaProperties.getSchemaLocation;
import static io.prestosql.plugin.iceberg.IcebergTableProperties.getFileFormat;
import static io.prestosql.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.prestosql.plugin.iceberg.IcebergTableProperties.getTableLocation;
import static io.prestosql.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.prestosql.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;

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
        this.hdfsEnvironment = hdfsEnvironment;
    }

    @Override
    public Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableIdentifier tableIdentifier = TableIdentifier.of(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        try {
            HadoopCatalog hadoopCatalog = getHadoopCatalog(session);
            return hadoopCatalog.loadTable(tableIdentifier);
        }
        catch(NoSuchTableException e) {
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
            throw new PrestoException(HIVE_METASTORE_ERROR, format("hive.metastore.warehouse.dir not set"));
        }
        return getHadoopCatalog(session, warehouse);
    }

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
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<PrestoPrincipal> getSchemaOwner(ConnectorSession session, CatalogSchemaName schemaName)
    {
        throw new UnsupportedOperationException();
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
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, PrestoPrincipal owner)
    {
        Optional<String> location = getSchemaLocation(properties).map(uri -> {
            try {
                hdfsEnvironment.getFileSystem(new HdfsContext(session, schemaName), new Path(uri));
            }
            catch (IOException | IllegalArgumentException e) {
                throw new PrestoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + uri, e);
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
    public void setSchemaAuthorization(ConnectorSession session, String source, PrestoPrincipal principal)
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
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        if (!icebergSchemaExists(session, schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }
        if (icebergTableExists(session, schemaTableName)) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        HdfsContext hdfsContext = new HdfsContext(session, schemaName, tableName);
        String targetPath = getTableLocation(tableMetadata.getProperties());
        HadoopCatalog catalog;
        if (targetPath == null) {
            catalog = getHadoopCatalog(session);
        }
        else {
            catalog = getHadoopCatalog(session, targetPath);
        }

        Schema schema = toIcebergSchema(tableMetadata.getColumns());
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));

        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builderWithExpectedSize(2);
        FileFormat fileFormat = getFileFormat(tableMetadata.getProperties());
        propertiesBuilder.put(DEFAULT_FILE_FORMAT, fileFormat.toString());
        if (tableMetadata.getComment().isPresent()) {
            propertiesBuilder.put(TABLE_COMMENT, tableMetadata.getComment().get());
        }
        Map<String, String> properties = propertiesBuilder.build();
        TableIdentifier tableIdentifier = TableIdentifier.of(schemaName, tableName);
        transaction = catalog.newCreateTableTransaction(tableIdentifier, schema, partitionSpec, properties);

        return new IcebergWritableTableHandle(
                schemaName,
                tableName,
                SchemaParser.toJson(transaction.table().schema()),
                PartitionSpecParser.toJson(transaction.table().spec()),
                getColumns(transaction.table().schema(), typeManager),
                transaction.table().location(),
                fileFormat);
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


//    protected ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName table)
//    {
//        if (!icebergTableExists(hdfsEnvironment, session, table)) {
//            throw new TableNotFoundException(table);
//        }
//
//        org.apache.iceberg.Table icebergTable = getIcebergTable(session, table);
//
//        List<ColumnMetadata> columns = getColumnMetadatas(icebergTable);
//
//        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
//        properties.put(FILE_FORMAT_PROPERTY, getFileFormat(icebergTable));
//        if (!icebergTable.spec().fields().isEmpty()) {
//            properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
//        }
//
//        return new ConnectorTableMetadata(table, columns, properties.build(), getTableComment(icebergTable));
//    }

//    private List<ColumnMetadata> getColumnMetadatas(org.apache.iceberg.Table table)
//    {
//        return table.schema().columns().stream()
//                .map(column -> {
//                    return ColumnMetadata.builder()
//                            .setName(column.name())
//                            .setType(toPrestoType(column.type(), typeManager))
//                            .setNullable(column.isOptional())
//                            .setComment(Optional.ofNullable(column.doc()))
//                            .build();
//                })
//                .collect(toImmutableList());
//    }

}
