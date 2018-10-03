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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveWrittenPartitions;
import io.prestosql.plugin.hive.TableAlreadyExistsException;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveColumnHandle.updateRowIdHandle;
import static io.prestosql.plugin.hive.HiveSchemaProperties.getLocation;
import static io.prestosql.plugin.hive.HiveWriteUtils.getTableDefaultLocation;
import static io.prestosql.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.prestosql.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.prestosql.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.prestosql.plugin.iceberg.IcebergTableProperties.getFileFormat;
import static io.prestosql.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.prestosql.plugin.iceberg.IcebergUtil.getColumns;
import static io.prestosql.plugin.iceberg.IcebergUtil.getDataPath;
import static io.prestosql.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.prestosql.plugin.iceberg.IcebergUtil.getIcebergTable;
import static io.prestosql.plugin.iceberg.IcebergUtil.isIcebergTable;
import static io.prestosql.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.prestosql.plugin.iceberg.PartitionFields.toPartitionFields;
import static io.prestosql.plugin.iceberg.TypeConveter.toIcebergType;
import static io.prestosql.plugin.iceberg.TypeConveter.toPrestoType;
import static io.prestosql.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.prestosql.spi.security.PrincipalType.USER;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.Transactions.createTableTransaction;

public class IcebergMetadata
        implements ConnectorMetadata
{
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;

    private Transaction transaction;

    public IcebergMetadata(
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableHandle handle = IcebergTableHandle.from(tableName);
        Optional<Table> table = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!table.isPresent()) {
            return null;
        }
        if (!isIcebergTable(table.get())) {
            throw new UnknownTableTypeException(tableName);
        }
        return handle;
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableHandle table = IcebergTableHandle.from(tableName);
        if (table.getTableType() == TableType.PARTITIONS) {
            Configuration configuration = getConfiguration(session, tableName.getSchemaName());
            org.apache.iceberg.Table icebergTable = getIcebergTable(table.getSchemaName(), table.getTableName(), configuration, metastore);
            return Optional.of(new PartitionTable(table, session, typeManager, icebergTable));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(session, ((IcebergTableHandle) table).getSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        // TODO: this should skip non-Iceberg tables
        return schemaName.map(Collections::singletonList)
                .orElseGet(metastore::getAllDatabases)
                .stream()
                .flatMap(schema -> metastore.getAllTables(schema).stream()
                        .map(table -> new SchemaTableName(schema, table))
                        .collect(toList())
                        .stream())
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Configuration configuration = getConfiguration(session, table.getSchemaName());
        org.apache.iceberg.Table icebergTable = getIcebergTable(table.getSchemaName(), table.getTableName(), configuration, metastore);
        List<HiveColumnHandle> columns = getColumns(icebergTable.schema(), icebergTable.spec(), typeManager);
        return columns.stream().collect(toMap(HiveColumnHandle::getName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        HiveColumnHandle column = (HiveColumnHandle) columnHandle;
        return new ColumnMetadata(column.getName(), typeManager.getType(column.getTypeSignature()));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTable()
                .map(ignored -> singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName table : tables) {
            try {
                columns.put(table, getTableMetadata(session, table).getColumns());
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
            catch (UnknownTableTypeException e) {
                // ignore table of unknown type
            }
        }
        return columns.build();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        Optional<String> location = getLocation(properties).map(uri -> {
            try {
                hdfsEnvironment.getFileSystem(new HdfsContext(session, schemaName), new Path(uri));
            }
            catch (IOException | IllegalArgumentException e) {
                throw new PrestoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + uri, e);
            }
            return uri;
        });

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setLocation(location)
                .setOwnerType(USER)
                .setOwnerName(session.getUser())
                .build();

        metastore.createDatabase(database);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        // basic sanity check to provide a better error message
        if (!listTables(session, Optional.of(schemaName)).isEmpty() ||
                !listViews(session, Optional.of(schemaName)).isEmpty()) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
        metastore.dropDatabase(schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        metastore.renameDatabase(source, target);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        Optional<ConnectorNewTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Schema schema = new Schema(toIceberg(tableMetadata.getColumns()));

        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));

        Database database = metastore.getDatabase(schemaName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));

        HdfsContext hdfsContext = new HdfsContext(session, schemaName, tableName);
        Path targetPath = getTableDefaultLocation(database, hdfsContext, hdfsEnvironment, schemaName, tableName);
        Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, targetPath);

        TableOperations operations = new HiveTableOperations(configuration, metastore, schemaName, tableName, session.getUser(), targetPath.toString());
        if (operations.current() != null) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        TableMetadata metadata = newTableMetadata(operations, schema, partitionSpec, targetPath.toString());

        transaction = createTableTransaction(operations, metadata);

        return new IcebergWritableTableHandle(
                schemaName,
                tableName,
                SchemaParser.toJson(metadata.schema()),
                PartitionSpecParser.toJson(metadata.spec()),
                getColumns(metadata.schema(), metadata.spec(), typeManager),
                targetPath.toString(),
                getFileFormat(tableMetadata.getProperties()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return finishInsert(session, (IcebergWritableTableHandle) tableHandle, fragments, computedStatistics);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Configuration configuration = getConfiguration(session, table.getSchemaName());
        org.apache.iceberg.Table icebergTable = getIcebergTable(table.getSchemaName(), table.getTableName(), configuration, metastore);

        for (NestedField column : icebergTable.schema().columns()) {
            io.prestosql.spi.type.Type type = toPrestoType(column.type(), typeManager);
            if (type instanceof DecimalType) {
                throw new PrestoException(NOT_SUPPORTED, "Writing to columns of type decimal not yet supported");
            }
            if (type instanceof TimestampType) {
                throw new PrestoException(NOT_SUPPORTED, "Writing to columns of type timestamp not yet supported");
            }
        }

        transaction = icebergTable.newTransaction();

        return new IcebergWritableTableHandle(
                table.getSchemaName(),
                table.getTableName(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                getColumns(icebergTable.schema(), icebergTable.spec(), typeManager),
                getDataPath(icebergTable.location()),
                getFileFormat(icebergTable));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;
        org.apache.iceberg.Table icebergTable = transaction.table();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = transaction.newFastAppend();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withInputFile(HadoopInputFile.fromLocation(task.getPath(), getConfiguration(session, table.getSchemaName())))
                    .withFormat(table.getFileFormat())
                    .withMetrics(task.getMetrics().metrics());

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
        }

        appendFiles.commit();
        transaction.commitTransaction();

        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::getPath)
                .collect(toImmutableList())));
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        return Optional.of(new IcebergInputInfo(table.getSnapshotId()));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        metastore.dropTable(handle.getSchemaName(), handle.getTableName(), true);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        metastore.renameTable(handle.getSchemaName(), handle.getTableName(), newTable.getSchemaName(), newTable.getTableName());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Configuration configuration = getConfiguration(session, handle.getSchemaName());
        org.apache.iceberg.Table icebergTable = getIcebergTable(handle.getSchemaName(), handle.getTableName(), configuration, metastore);
        icebergTable.updateSchema().addColumn(column.getName(), toIcebergType(column.getType())).commit();
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        HiveColumnHandle handle = (HiveColumnHandle) column;
        Configuration configuration = getConfiguration(session, icebergTableHandle.getSchemaName());
        org.apache.iceberg.Table icebergTable = getIcebergTable(icebergTableHandle.getSchemaName(), icebergTableHandle.getTableName(), configuration, metastore);
        icebergTable.updateSchema().deleteColumn(handle.getName()).commit();
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        HiveColumnHandle columnHandle = (HiveColumnHandle) source;
        Configuration configuration = getConfiguration(session, icebergTableHandle.getSchemaName());
        org.apache.iceberg.Table icebergTable = getIcebergTable(icebergTableHandle.getSchemaName(), icebergTableHandle.getTableName(), configuration, metastore);
        icebergTable.updateSchema().renameColumn(columnHandle.getName(), target).commit();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName schemaTableName)
    {
        String schema = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        if (!metastore.getTable(schema, tableName).isPresent()) {
            throw new TableNotFoundException(schemaTableName);
        }

        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsContext(session, schema), new Path("file:///tmp"));

        org.apache.iceberg.Table icebergTable = getIcebergTable(schema, tableName, configuration, metastore);

        List<ColumnMetadata> columns = getColumnMetadatas(icebergTable);

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(FILE_FORMAT_PROPERTY, getFileFormat(icebergTable));
        if (!icebergTable.spec().fields().isEmpty()) {
            properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
        }

        return new ConnectorTableMetadata(schemaTableName, columns, properties.build(), Optional.empty());
    }

    private List<ColumnMetadata> getColumnMetadatas(org.apache.iceberg.Table table)
    {
        return table.schema().columns().stream()
                .map(column -> new ColumnMetadata(column.name(), toPrestoType(column.type(), typeManager)))
                .collect(toImmutableList());
    }

    private static List<NestedField> toIceberg(List<ColumnMetadata> columns)
    {
        List<NestedField> icebergColumns = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            if (!column.isHidden()) {
                int index = icebergColumns.size();
                Type type = toIcebergType(column.getType());
                NestedField field = column.isNullable()
                        ? NestedField.optional(index, column.getName(), type, column.getComment())
                        : NestedField.required(index, column.getName(), type, column.getComment());
                icebergColumns.add(field);
            }
        }
        return icebergColumns;
    }

    private Configuration getConfiguration(ConnectorSession session, String schemaName)
    {
        return hdfsEnvironment.getConfiguration(new HdfsContext(session, schemaName), new Path("file:///tmp"));
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return updateRowIdHandle();
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.of(handle);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector only supports delete where one or more partitions are deleted entirely");
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;

        Configuration configuration = getConfiguration(session, handle.getSchemaName());
        org.apache.iceberg.Table icebergTable = getIcebergTable(handle.getSchemaName(), handle.getTableName(), configuration, metastore);
        icebergTable.newDelete()
                .deleteFromRowFilter(toIcebergExpression(handle.getPredicate(), session))
                .commit();

        // TODO: it should be possible to return number of deleted records
        return OptionalLong.empty();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    public HiveMetastore getMetastore()
    {
        return metastore;
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }
}
