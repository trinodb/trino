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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.plugin.kudu.properties.KuduTableProperties;
import io.trino.plugin.kudu.properties.PartitionDesign;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTablePartitioning;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.NotFoundException;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartitionSchema.HashBucketSchema;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.kudu.KuduColumnHandle.ROW_ID;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
import static java.util.Objects.requireNonNull;

public class KuduMetadata
        implements ConnectorMetadata
{
    private final KuduClientSession clientSession;

    @Inject
    public KuduMetadata(KuduClientSession clientSession)
    {
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return clientSession.listSchemaNames();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return clientSession.listTables(schemaName);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        List<SchemaTableName> tables;
        if (prefix.getTable().isEmpty()) {
            tables = listTables(session, prefix.getSchema());
        }
        else {
            tables = ImmutableList.of(prefix.toSchemaTableName());
        }

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : tables) {
            KuduTableHandle tableHandle = getTableHandle(session, tableName);
            if (tableHandle != null) {
                ConnectorTableMetadata tableMetadata = getTableMetadata(tableHandle);
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    private ColumnMetadata getColumnMetadata(ColumnSchema column)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        StringBuilder extra = new StringBuilder();
        if (column.isKey()) {
            properties.put(KuduTableProperties.PRIMARY_KEY, true);
            extra.append("primary_key, ");
        }

        if (column.isNullable()) {
            properties.put(KuduTableProperties.NULLABLE, true);
            extra.append("nullable, ");
        }

        String encoding = KuduTableProperties.lookupEncodingString(column.getEncoding());
        if (column.getEncoding() != ColumnSchema.Encoding.AUTO_ENCODING) {
            properties.put(KuduTableProperties.ENCODING, encoding);
        }
        extra.append("encoding=").append(encoding).append(", ");

        String compression = KuduTableProperties.lookupCompressionString(column.getCompressionAlgorithm());
        if (column.getCompressionAlgorithm() != ColumnSchema.CompressionAlgorithm.DEFAULT_COMPRESSION) {
            properties.put(KuduTableProperties.COMPRESSION, compression);
        }
        extra.append("compression=").append(compression);

        Type prestoType = TypeHelper.fromKuduColumn(column);
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(prestoType)
                .setExtraInfo(Optional.of(extra.toString()))
                .setProperties(properties)
                .setComment(Optional.ofNullable(column.getComment()))
                .build();
    }

    private ConnectorTableMetadata getTableMetadata(KuduTableHandle tableHandle)
    {
        KuduTable table = tableHandle.getTable(clientSession);
        Schema schema = table.getSchema();

        List<ColumnMetadata> columnsMetaList = schema.getColumns().stream()
                .filter(column -> !column.isKey() || !column.getName().equals(ROW_ID))
                .map(this::getColumnMetadata)
                .collect(toImmutableList());

        Map<String, Object> properties = clientSession.getTableProperties(tableHandle);
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), columnsMetaList, properties);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle connectorTableHandle)
    {
        KuduTableHandle tableHandle = (KuduTableHandle) connectorTableHandle;
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        Schema schema = clientSession.getTableSchema(tableHandle);
        forAllColumnHandles(schema, column -> columnHandles.put(column.getName(), column));
        return columnHandles.buildOrThrow();
    }

    private void forAllColumnHandles(Schema schema, Consumer<KuduColumnHandle> handleEater)
    {
        for (int ordinal = 0; ordinal < schema.getColumnCount(); ordinal++) {
            ColumnSchema col = schema.getColumnByIndex(ordinal);
            String name = col.getName();
            Type type = TypeHelper.fromKuduColumn(col);
            KuduColumnHandle columnHandle = new KuduColumnHandle(name, ordinal, type);
            handleEater.accept(columnHandle);
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        KuduColumnHandle kuduColumnHandle = (KuduColumnHandle) columnHandle;
        if (kuduColumnHandle.isVirtualRowId()) {
            return ColumnMetadata.builder()
                    .setName(ROW_ID)
                    .setType(VarbinaryType.VARBINARY)
                    .setHidden(true)
                    .build();
        }
        return kuduColumnHandle.getColumnMetadata();
    }

    @Override
    public KuduTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try {
            KuduTable table = clientSession.openTable(schemaTableName);
            OptionalInt bucketCount = OptionalInt.empty();
            List<HashBucketSchema> bucketSchemas = table.getPartitionSchema().getHashBucketSchemas();
            if (!bucketSchemas.isEmpty()) {
                bucketCount = OptionalInt.of(bucketSchemas.stream()
                        .mapToInt(HashBucketSchema::getNumBuckets)
                        .reduce(1, Math::multiplyExact));
            }
            return new KuduTableHandle(schemaTableName, table, TupleDomain.all(), Optional.empty(), false, bucketCount, OptionalLong.empty());
        }
        catch (NotFoundException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        return getTableMetadata(kuduTableHandle);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        clientSession.createSchema(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        clientSession.dropSchema(schemaName);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }
        if (tableMetadata.getColumns().stream().anyMatch(column -> column.getComment() != null)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
        }
        clientSession.createTable(tableMetadata, ignoreExisting);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        clientSession.dropTable(kuduTableHandle.getSchemaTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        clientSession.renameTable(kuduTableHandle.getSchemaTableName(), newTableName);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        clientSession.addColumn(kuduTableHandle.getSchemaTableName(), column);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        KuduColumnHandle kuduColumnHandle = (KuduColumnHandle) column;
        clientSession.dropColumn(kuduTableHandle.getSchemaTableName(), kuduColumnHandle.getName());
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        KuduColumnHandle kuduColumnHandle = (KuduColumnHandle) source;
        clientSession.renameColumn(kuduTableHandle.getSchemaTableName(), kuduColumnHandle.getName(), target);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle connectorTableHandle, List<ColumnHandle> insertedColumns, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }

        KuduTableHandle tableHandle = (KuduTableHandle) connectorTableHandle;

        KuduTable table = tableHandle.getTable(clientSession);
        Schema schema = table.getSchema();

        List<ColumnSchema> columns = schema.getColumns();
        List<Type> columnTypes = columns.stream()
                .map(TypeHelper::fromKuduColumn).collect(toImmutableList());

        return new KuduInsertTableHandle(
                tableHandle.getSchemaTableName(),
                columnTypes,
                columns.stream()
                        .anyMatch(column -> column.getName().equals(ROW_ID)),
                table);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout,
            RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }
        PartitionDesign design = KuduTableProperties.getPartitionDesign(tableMetadata.getProperties());
        boolean generateUUID = !design.hasPartitions();
        ConnectorTableMetadata finalTableMetadata = tableMetadata;
        if (generateUUID) {
            String rowId = ROW_ID;
            List<ColumnMetadata> copy = new ArrayList<>(tableMetadata.getColumns());
            Map<String, Object> columnProperties = new HashMap<>();
            columnProperties.put(KuduTableProperties.PRIMARY_KEY, true);
            copy.add(0, ColumnMetadata.builder()
                    .setName(rowId)
                    .setType(VarcharType.VARCHAR)
                    .setComment(Optional.of("key=true"))
                    .setHidden(true)
                    .setProperties(columnProperties)
                    .build());
            List<ColumnMetadata> finalColumns = ImmutableList.copyOf(copy);
            Map<String, Object> propsCopy = new HashMap<>(tableMetadata.getProperties());
            propsCopy.put(KuduTableProperties.PARTITION_BY_HASH_COLUMNS, ImmutableList.of(rowId));
            propsCopy.put(KuduTableProperties.PARTITION_BY_HASH_BUCKETS, 2);
            Map<String, Object> finalProperties = ImmutableMap.copyOf(propsCopy);
            finalTableMetadata = new ConnectorTableMetadata(tableMetadata.getTable(),
                    finalColumns, finalProperties, tableMetadata.getComment());
        }
        KuduTable table = clientSession.createTable(finalTableMetadata, false);

        Schema schema = table.getSchema();

        List<ColumnSchema> columns = schema.getColumns();
        List<Type> columnTypes = columns.stream()
                .map(TypeHelper::fromKuduColumn).collect(toImmutableList());
        List<Type> columnOriginalTypes = finalTableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType).collect(toImmutableList());

        return new KuduOutputTableHandle(
                finalTableMetadata.getTable(),
                columnOriginalTypes,
                columnTypes,
                generateUUID,
                table);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return CHANGE_ONLY_UPDATED_COLUMNS;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return KuduColumnHandle.ROW_ID_HANDLE;
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        KuduTable table = kuduTableHandle.getTable(clientSession);
        Schema schema = table.getSchema();
        List<ColumnSchema> columns = schema.getColumns();
        List<Type> columnTypes = columns.stream()
                .map(TypeHelper::fromKuduColumn)
                .collect(toImmutableList());
        ConnectorTableMetadata tableMetadata = getTableMetadata(kuduTableHandle);
        List<Type> columnOriginalTypes = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        PartitionDesign design = KuduTableProperties.getPartitionDesign(tableMetadata.getProperties());
        boolean generateUUID = !design.hasPartitions();
        return new KuduMergeTableHandle(
                kuduTableHandle.withRequiresRowId(true),
                new KuduOutputTableHandle(tableMetadata.getTable(), columnOriginalTypes, columnTypes, generateUUID, table));
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        // For Kudu, nothing needs to be done finish the merge.
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        KuduTableHandle handle = (KuduTableHandle) table;

        Optional<ConnectorTablePartitioning> tablePartitioning = Optional.empty();
        Optional<Set<ColumnHandle>> partitioningColumns = Optional.empty();
        List<LocalProperty<ColumnHandle>> localProperties = ImmutableList.of();

        return new ConnectorTableProperties(
                handle.getConstraint(),
                tablePartitioning,
                partitioningColumns,
                Optional.empty(),
                localProperties);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        KuduTableHandle handle = (KuduTableHandle) table;

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new KuduTableHandle(
                handle.getSchemaTableName(),
                handle.getTable(clientSession),
                newDomain,
                handle.getDesiredColumns(),
                handle.isRequiresRowId(),
                handle.getBucketCount(),
                handle.getLimit());

        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary(), false));
    }

    /**
     * Only applies to the projection which selects a list of top-level columns.
     * <p>
     * Take this query "select col1, col2.field1 from test_table" as an example:
     * <p>
     * The optimizer calls with the following arguments:
     * <p>
     * handle = TH0 (col0, col1, col2, col3)
     * projections = [
     * col1,
     * f(col2)
     * ]
     * assignments = [
     * col1 = CH1
     * col2 = CH2
     * ]
     * <p>
     * <p>
     * This method returns:
     * <p>
     * handle = TH1 (col1, col2)
     * projections = [
     * col1,
     * f(col2)
     * ]
     * assignments = [
     * col1 = CH1
     * col2 = CH2
     * ]
     */
    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        KuduTableHandle handle = (KuduTableHandle) table;

        if (handle.getDesiredColumns().isPresent()) {
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnHandle> desiredColumns = ImmutableList.builder();
        ImmutableList.Builder<Assignment> assignmentList = ImmutableList.builder();
        assignments.forEach((name, column) -> {
            desiredColumns.add(column);
            assignmentList.add(new Assignment(name, column, ((KuduColumnHandle) column).getType()));
        });

        handle = new KuduTableHandle(
                handle.getSchemaTableName(),
                handle.getTable(clientSession),
                handle.getConstraint(),
                Optional.of(desiredColumns.build()),
                handle.isRequiresRowId(),
                handle.getBucketCount(),
                handle.getLimit());

        return Optional.of(new ProjectionApplicationResult<>(handle, projections, assignmentList.build(), false));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        KuduTableHandle handle = (KuduTableHandle) table;

        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        handle = new KuduTableHandle(
                handle.getSchemaTableName(),
                handle.getTable(clientSession),
                handle.getConstraint(),
                handle.getDesiredColumns(),
                handle.isRequiresRowId(),
                handle.getBucketCount(),
                OptionalLong.of(limit));

        return Optional.of(new LimitApplicationResult<>(handle, false, false));
    }
}
