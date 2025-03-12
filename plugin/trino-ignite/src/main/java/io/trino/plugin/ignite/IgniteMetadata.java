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
package io.trino.plugin.ignite;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.JdbcMetadata.getColumns;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.SaveMode.REPLACE;
import static java.util.Objects.requireNonNull;

public class IgniteMetadata
        extends DefaultJdbcMetadata
{
    private static final String IGNITE_DUMMY_ID = "dummy_id";
    private final JdbcClient igniteClient;

    public IgniteMetadata(JdbcClient igniteClient, TimestampTimeZoneDomain timestampTimeZoneDomain, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(igniteClient, timestampTimeZoneDomain, false, jdbcQueryEventListeners);
        this.igniteClient = requireNonNull(igniteClient, "igniteClient is null");
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        return igniteClient.getTableHandle(session, schemaTableName)
                .map(JdbcTableHandle::asPlainTable)
                .map(JdbcNamedRelationHandle::getRemoteTableName)
                .map(remoteTableName -> new JdbcTableHandle(
                        schemaTableName,
                        new RemoteTableName(remoteTableName.getCatalogName(), Optional.of(remoteTableName.getSchemaName().orElse("public")), remoteTableName.getTableName()),
                        Optional.empty()))
                .orElse(null);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        Optional<String> dummyIdColumn = getColumns(session, igniteClient, handle).stream()
                .map(JdbcColumnHandle::getColumnName)
                .filter(IGNITE_DUMMY_ID::equalsIgnoreCase)
                .findFirst();

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        ImmutableList.Builder<JdbcTypeHandle> columnJdbcTypeHandles = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
            if (IGNITE_DUMMY_ID.equalsIgnoreCase(columnHandle.getColumnName())) {
                continue;
            }
            columnNames.add(columnHandle.getColumnName());
            columnTypes.add(columnHandle.getColumnType());
            columnJdbcTypeHandles.add(columnHandle.getJdbcTypeHandle());
        }

        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        return new IgniteOutputTableHandle(
                remoteTableName,
                columnNames.build(),
                columnTypes.build(),
                Optional.of(columnJdbcTypeHandles.build()),
                dummyIdColumn);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, Map<Integer, Collection<ColumnHandle>> updateColumnHandles, RetryMode retryMode)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        JdbcMergeTableHandle mergeTableHandle = (JdbcMergeTableHandle) super.beginMerge(session, tableHandle, updateColumnHandles, retryMode);

        List<JdbcColumnHandle> primaryKeys = mergeTableHandle.getPrimaryKeys();
        List<JdbcColumnHandle> columns = igniteClient.getColumns(session,
                        handle.getRequiredNamedRelation().getSchemaTableName(),
                        handle.getRequiredNamedRelation().getRemoteTableName()).stream()
                .filter(column -> !IGNITE_DUMMY_ID.equalsIgnoreCase(column.getColumnName()))
                .collect(toImmutableList());

        for (Collection<ColumnHandle> updateColumns : updateColumnHandles.values()) {
            for (ColumnHandle column : updateColumns) {
                checkArgument(columns.contains(column), "the update column not found in the target table");
                checkArgument(!primaryKeys.contains(column), "Ignite does not allow update primary key");
            }
        }

        if (handle.getColumns().isPresent()) {
            handle = new JdbcTableHandle(
                    handle.getRelationHandle(),
                    handle.getConstraint(),
                    handle.getConstraintExpressions(),
                    handle.getSortOrder(),
                    handle.getLimit(),
                    Optional.of(columns),
                    handle.getOtherReferencedTables(),
                    handle.getNextSyntheticColumnId(),
                    handle.getAuthorization(),
                    handle.getUpdateAssignments());
        }

        return new IgniteMergeTableHandle(
                handle,
                (IgniteOutputTableHandle) mergeTableHandle.getOutputTableHandle(),
                primaryKeys,
                columns,
                mergeTableHandle.getUpdateCaseColumns());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        return new ConnectorTableMetadata(
                handle.getRequiredNamedRelation().getSchemaTableName(),
                getColumnMetadata(session, handle),
                igniteClient.getTableProperties(session, handle));
    }

    @Override
    public List<ColumnMetadata> getColumnMetadata(ConnectorSession session, JdbcTableHandle handle)
    {
        return getColumns(session, igniteClient, handle).stream()
                .filter(column -> !IGNITE_DUMMY_ID.equalsIgnoreCase(column.getColumnName()))
                .map(JdbcColumnHandle::getColumnMetadata)
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        return new ConnectorTableSchema(
                handle.getRequiredNamedRelation().getSchemaTableName(),
                getColumnMetadata(session, handle).stream()
                        .map(ColumnMetadata::getColumnSchema)
                        .collect(toImmutableList()));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        if (saveMode == REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        igniteClient.beginCreateTable(session, tableMetadata);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        return igniteClient.beginCreateTable(session, tableMetadata);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping a not null constraint");
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle table, ColumnMetadata columnMetadata, ColumnPosition position)
    {
        if (!columnMetadata.isNullable()) {
            // https://issues.apache.org/jira/browse/IGNITE-18829
            // Add not null column to non-empty table Ignite doesn't give the default value
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding not null columns");
        }

        switch (position) {
            case ColumnPosition.First _ -> throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with FIRST clause");
            case ColumnPosition.After _ -> throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with AFTER clause");
            case ColumnPosition.Last _ -> {
                JdbcTableHandle handle = (JdbcTableHandle) table;
                igniteClient.addColumn(session, handle, columnMetadata, position);
            }
        }
    }
}
