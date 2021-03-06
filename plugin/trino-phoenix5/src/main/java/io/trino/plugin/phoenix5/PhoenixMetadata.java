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
package io.trino.plugin.phoenix5;

import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorNewTableLayout;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.phoenix5.MetadataUtil.getEscapedTableName;
import static io.trino.plugin.phoenix5.MetadataUtil.toPrestoSchemaName;
import static io.trino.plugin.phoenix5.PhoenixErrorCode.PHOENIX_METADATA_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.phoenix.util.SchemaUtil.getEscapedArgument;

public class PhoenixMetadata
        extends JdbcMetadata
{
    // Maps to Phoenix's default empty schema
    public static final String DEFAULT_SCHEMA = "default";
    // col name used for PK if none provided in DDL
    private static final String ROWKEY = "ROWKEY";
    private final PhoenixClient phoenixClient;

    @Inject
    public PhoenixMetadata(PhoenixClient phoenixClient, JdbcMetadataConfig metadataConfig)
    {
        super(phoenixClient, metadataConfig.isAllowDropTable());
        this.phoenixClient = requireNonNull(phoenixClient, "client is null");
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return phoenixClient.getTableHandle(session, schemaTableName)
                .map(tableHandle -> new JdbcTableHandle(
                        schemaTableName,
                        tableHandle.getCatalogName(),
                        toPrestoSchemaName(Optional.ofNullable(tableHandle.getSchemaName())).orElse(null),
                        tableHandle.getTableName()))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(session, table, false);
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table, boolean rowkeyRequired)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        List<ColumnMetadata> columnMetadata = phoenixClient.getColumns(session, handle).stream()
                .filter(column -> rowkeyRequired || !ROWKEY.equalsIgnoreCase(column.getColumnName()))
                .map(JdbcColumnHandle::getColumnMetadata)
                .collect(toImmutableList());
        return new ConnectorTableMetadata(handle.getRequiredNamedRelation().getSchemaTableName(), columnMetadata, phoenixClient.getTableProperties(session, handle));
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        checkArgument(properties.isEmpty(), "Can't have properties for schema creation");
        if (DEFAULT_SCHEMA.equalsIgnoreCase(schemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "Can't create 'default' schema which maps to Phoenix empty schema");
        }
        phoenixClient.execute(session, format("CREATE SCHEMA %s", getEscapedArgument(toMetadataCasing(session, schemaName))));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        if (DEFAULT_SCHEMA.equalsIgnoreCase(schemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "Can't drop 'default' schema which maps to Phoenix empty schema");
        }
        phoenixClient.execute(session, format("DROP SCHEMA %s", getEscapedArgument(toMetadataCasing(session, schemaName))));
    }

    private String toMetadataCasing(ConnectorSession session, String schemaName)
    {
        try (Connection connection = phoenixClient.getConnection(session)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schemaName = schemaName.toUpperCase(ENGLISH);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(PHOENIX_METADATA_ERROR, "Couldn't get casing for the schema name", e);
        }
        return schemaName;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        phoenixClient.beginCreateTable(session, tableMetadata);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        return phoenixClient.beginCreateTable(session, tableMetadata);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        return true;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        Optional<String> rowkeyColumn = phoenixClient.getColumns(session, handle).stream()
                .map(JdbcColumnHandle::getColumnName)
                .filter(ROWKEY::equalsIgnoreCase)
                .findFirst();

        List<JdbcColumnHandle> columnHandles = columns.stream()
                .map(JdbcColumnHandle.class::cast)
                .collect(toImmutableList());

        return new PhoenixOutputTableHandle(
                Optional.ofNullable(handle.getSchemaName()),
                handle.getTableName(),
                columnHandles.stream().map(JdbcColumnHandle::getColumnName).collect(toImmutableList()),
                columnHandles.stream().map(JdbcColumnHandle::getColumnType).collect(toImmutableList()),
                Optional.of(columnHandles.stream().map(JdbcColumnHandle::getJdbcTypeHandle).collect(toImmutableList())),
                rowkeyColumn);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        phoenixClient.execute(session, format(
                "ALTER TABLE %s ADD %s %s",
                getEscapedTableName(Optional.ofNullable(handle.getSchemaName()), handle.getTableName()),
                column.getName(),
                phoenixClient.toWriteMapping(session, column.getType()).getDataType()));
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
        phoenixClient.execute(session, format(
                "ALTER TABLE %s DROP COLUMN %s",
                getEscapedTableName(Optional.ofNullable(handle.getSchemaName()), handle.getTableName()),
                columnHandle.getColumnName()));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // if we autogenerated a ROWKEY for this table, delete the associated sequence as well
        boolean hasRowkey = getColumnHandles(session, tableHandle).values().stream()
                .map(JdbcColumnHandle.class::cast)
                .map(JdbcColumnHandle::getColumnName)
                .anyMatch(ROWKEY::equals);
        if (hasRowkey) {
            JdbcTableHandle jdbcHandle = (JdbcTableHandle) tableHandle;
            phoenixClient.execute(session, format("DROP SEQUENCE %s", getEscapedTableName(Optional.ofNullable(jdbcHandle.getSchemaName()), jdbcHandle.getTableName() + "_sequence")));
        }
        phoenixClient.dropTable(session, (JdbcTableHandle) tableHandle);
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        // TODO support aggregation pushdown
        return Optional.empty();
    }
}
