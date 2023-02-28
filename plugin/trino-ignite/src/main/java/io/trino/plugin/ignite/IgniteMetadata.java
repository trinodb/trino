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
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static java.util.Objects.requireNonNull;

public class IgniteMetadata
        extends DefaultJdbcMetadata
{
    private static final String IGNITE_DUMMY_ID = "dummy_id";
    private final JdbcClient igniteClient;

    @Inject
    public IgniteMetadata(JdbcClient igniteClient, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(igniteClient, false, jdbcQueryEventListeners);
        this.igniteClient = requireNonNull(igniteClient, "igniteClient is null");
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
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
        Optional<String> dummyIdColumn = igniteClient.getColumns(session, handle).stream()
                .map(JdbcColumnHandle::getColumnName)
                .filter(IGNITE_DUMMY_ID::equalsIgnoreCase)
                .findFirst();

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        ImmutableList.Builder<JdbcTypeHandle> columnJdbcTypeHandles = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
            columnNames.add(columnHandle.getColumnName());
            columnTypes.add(columnHandle.getColumnType());
            columnJdbcTypeHandles.add(columnHandle.getJdbcTypeHandle());
        }

        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        return new IgniteOutputTableHandle(
                remoteTableName.getSchemaName().orElse(null),
                remoteTableName.getTableName(),
                columnNames.build(),
                columnTypes.build(),
                Optional.of(columnJdbcTypeHandles.build()),
                dummyIdColumn);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        return new ConnectorTableMetadata(
                getSchemaTableName(handle),
                getColumnMetadata(session, handle),
                igniteClient.getTableProperties(session, handle));
    }

    private List<ColumnMetadata> getColumnMetadata(ConnectorSession session, JdbcTableHandle handle)
    {
        return igniteClient.getColumns(session, handle).stream()
                .filter(column -> !IGNITE_DUMMY_ID.equalsIgnoreCase(column.getColumnName()))
                .map(JdbcColumnHandle::getColumnMetadata)
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        return new ConnectorTableSchema(
                getSchemaTableName(handle),
                getColumnMetadata(session, handle).stream()
                        .map(ColumnMetadata::getColumnSchema)
                        .collect(toImmutableList()));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        igniteClient.beginCreateTable(session, tableMetadata);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
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
    public void addColumn(ConnectorSession session, ConnectorTableHandle table, ColumnMetadata columnMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }
}
