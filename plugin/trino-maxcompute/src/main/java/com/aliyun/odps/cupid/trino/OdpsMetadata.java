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
package com.aliyun.odps.cupid.trino;

import com.aliyun.odps.cupid.table.v1.writer.TableWriteSession;
import com.aliyun.odps.cupid.table.v1.writer.TableWriteSessionBuilder;
import com.aliyun.odps.cupid.table.v1.writer.WriteSessionInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.commons.codec.binary.Base64;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class OdpsMetadata
        implements ConnectorMetadata {
    private static final String PARTITIONS_TABLE_SUFFIX = "$partitions";

    private final String connectorId;
    private final OdpsClient odpsClient;
    private Map<String, TableWriteSession> tableWriteSessionMap;
    private static final Logger log = Logger.get(OdpsMetadata.class);

    @Inject
    public OdpsMetadata(OdpsConnectorId connectorId, OdpsClient odpsClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.odpsClient = requireNonNull(odpsClient, "client is null");
        this.tableWriteSessionMap = new ConcurrentHashMap<>(2);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return listSchemaNames();
    }

    public List<String> listSchemaNames() {
        return ImmutableList.copyOf(odpsClient.getProjectNames());
    }

    @Override
    public OdpsTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        OdpsTable table = odpsClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new OdpsTableHandle(tableName.getSchemaName(), tableName.getTableName(), table, ImmutableList.of());
    }

//    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session,
//        ConnectorTableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments) {
//        OdpsTableHandle handle = (OdpsTableHandle) table;
//        if (handle.getDesiredColumns().size() > 0) {
//            return Optional.empty();
//        }
//        ImmutableList.Builder<OdpsColumnHandle> desiredColumns = ImmutableList.builder();
//        ImmutableList.Builder<Assignment> assignmentList = ImmutableList.builder();
//        assignments.forEach((name, column) -> {
//            desiredColumns.add((OdpsColumnHandle) column);
//            assignmentList.add(new Assignment(name, column, ((OdpsColumnHandle) column).getType()));
//        });
//        return Optional.of(new ProjectionApplicationResult<>(handle, projections, assignmentList.build(), false));
//    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        OdpsTableHandle odpsTableHandle = (OdpsTableHandle) table;
        SchemaTableName tableName = new SchemaTableName(odpsTableHandle.getSchemaName(), odpsTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull) {
        Set<String> schemaNames;
        if (schemaNameOrNull.isPresent()) {
            schemaNames = ImmutableSet.of(schemaNameOrNull.get());
        } else {
            schemaNames = odpsClient.getProjectNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : odpsClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        OdpsTableHandle odpsTableHandle = (OdpsTableHandle) tableHandle;
        OdpsTable table = odpsClient.getTable(odpsTableHandle.getSchemaName(), odpsTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(odpsTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new OdpsColumnHandle(column.getName(), column.getType(),
                    column.getType() instanceof VarcharType && ((VarcharType) column.getType()).isUnbounded()));
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace) {
        String schemaName = tableMetadata.getTable().getSchemaName();
        String tableName = tableMetadata.getTable().getTableName();
        List<OdpsColumnHandle> inputColumns = tableMetadata.getColumns().stream()
                .map(e -> new OdpsColumnHandle(e.getName(), e.getType(), false))
                .collect(toList());
        List<String> partitionedBy = ImmutableList.of();

        return new OdpsOutputTableHandle(
                schemaName,
                tableName,
                inputColumns,
                partitionedBy);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics) {
        return Optional.empty();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode) {
        String projectName = tableMetadata.getTable().getSchemaName();
        String tableName = tableMetadata.getTable().getTableName();
        List<OdpsColumnHandle> inputColumns = tableMetadata.getColumns().stream()
                .map(e -> new OdpsColumnHandle(e.getName(), e.getType(), false))
                .collect(toList());
        List<String> partitionedBy = ImmutableList.of();

        boolean ignoreExisting = (saveMode == SaveMode.IGNORE);
        odpsClient.createTable(projectName, tableName, inputColumns, partitionedBy, ignoreExisting);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
        OdpsTableHandle odpsTableHandle = (OdpsTableHandle) tableHandle;
        odpsClient.dropTable(odpsTableHandle.getSchemaName(), odpsTableHandle.getTableName());
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode) {
        OdpsTableHandle odpsTableHandle = (OdpsTableHandle) tableHandle;
        String tableApiProvider;
        if (System.getenv("META_LOOKUP_NAME") != null) {
            tableApiProvider = "cupid-native";
        } else {
            tableApiProvider = "tunnel";
        }

        try {
            TableWriteSession tableWriteSession = new TableWriteSessionBuilder(tableApiProvider,
                    odpsTableHandle.getSchemaName(),
                    odpsTableHandle.getTableName()).tableSchema(
                            odpsClient.getTableSchema(odpsTableHandle.getSchemaName(), odpsTableHandle.getTableName()))
                    .build();
            WriteSessionInfo writeSessionInfo = tableWriteSession.getOrCreateSessionInfo();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(baos);
            out.writeObject(writeSessionInfo);
            String sessionInfoBase64Str = Base64.encodeBase64String(baos.toByteArray());
            tableWriteSessionMap.put(sessionInfoBase64Str, tableWriteSession);

            return new OdpsInsertTableHandle(
                    odpsTableHandle.getSchemaName(),
                    odpsTableHandle.getTableName(),
                    odpsTableHandle.getOdpsTable(),
                    sessionInfoBase64Str,
                    tableApiProvider,
                    tableWriteSession);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics) {
        OdpsInsertTableHandle odpsInsertTableHandle = (OdpsInsertTableHandle) insertHandle;
        TableWriteSession tableWriteSession = tableWriteSessionMap.get(odpsInsertTableHandle.getWriteSessionInfo());
        try {
            tableWriteSession.commitTable(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            tableWriteSessionMap.remove(odpsInsertTableHandle.getWriteSessionInfo());
        }
        return Optional.empty();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName) {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        OdpsTable table = odpsClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
        return connectorTableMetadata;
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
        if (!prefix.getSchema().isPresent()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchema().get(), prefix.getTable().get()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((OdpsColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName) {
        if (isPartitionsSystemTable(tableName)) {
            return getPartitionsSystemTable(session, tableName);
        }
        return Optional.empty();
    }

    private Optional<SystemTable> getPartitionsSystemTable(ConnectorSession session, SchemaTableName tableName) {
        SchemaTableName sourceTableName = getSourceTableNameForPartitionsTable(tableName);
        OdpsTableHandle sourceTableHandle = getTableHandle(session, sourceTableName, Optional.empty(), Optional.empty());

        if (sourceTableHandle == null) {
            return Optional.empty();
        }

        List<OdpsColumnHandle> partitionColumns = sourceTableHandle.getOdpsTable().getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return Optional.empty();
        }

        List<Type> partitionColumnTypes = partitionColumns.stream()
                .map(e -> e.getType())
                .collect(toImmutableList());

        List<ColumnMetadata> partitionSystemTableColumns = partitionColumns.stream()
                .map(column -> new ColumnMetadata(
                        column.getName(),
                        column.getType()))
                .collect(toImmutableList());

        SystemTable partitionsSystemTable = new SystemTable() {
            @Override
            public Distribution getDistribution() {
                return Distribution.SINGLE_COORDINATOR;
            }

            @Override
            public ConnectorTableMetadata getTableMetadata() {
                return new ConnectorTableMetadata(tableName, partitionSystemTableColumns);
            }

            @Override
            public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint) {
                Iterable<List<Object>> records = () ->
                        odpsClient.getOdpsPartitions(sourceTableName.getSchemaName(), sourceTableName.getTableName(),
                                        sourceTableHandle.getOdpsTable(), Constraint.alwaysTrue())
                                .stream()
                                .map(odpsPartition ->
                                        IntStream.range(0, partitionColumns.size())
                                                .mapToObj(partitionColumns::get)
                                                .map(columnHandle -> odpsPartition.getKeys().get(columnHandle).getValue())
                                                .collect(toList()))
                                .iterator();

                return new InMemoryRecordSet(partitionColumnTypes, records).cursor();
            }
        };
        return Optional.of(partitionsSystemTable);
    }

    public static boolean isPartitionsSystemTable(SchemaTableName tableName) {
        return tableName.getTableName().endsWith(PARTITIONS_TABLE_SUFFIX) && tableName.getTableName().length() > PARTITIONS_TABLE_SUFFIX.length();
    }

    public static SchemaTableName getSourceTableNameForPartitionsTable(SchemaTableName tableName) {
        checkArgument(isPartitionsSystemTable(tableName), "not a partitions table name");
        return new SchemaTableName(
                tableName.getSchemaName(),
                tableName.getTableName().substring(0, tableName.getTableName().length() - PARTITIONS_TABLE_SUFFIX.length()));
    }
}
