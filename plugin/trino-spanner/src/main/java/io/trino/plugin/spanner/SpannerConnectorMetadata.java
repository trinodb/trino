package io.trino.plugin.spanner;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SpannerConnectorMetadata
        extends DefaultJdbcMetadata
{

    public static final String DEFAULT_SCHEMA = "default";
    private final SpannerClient spannerClient;
    private final IdentifierMapping identifierMapping;

    @Inject
    public SpannerConnectorMetadata(SpannerClient spannerClient,
            IdentifierMapping identifierMapping,
            Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(spannerClient, false, jdbcQueryEventListeners);
        this.spannerClient = requireNonNull(spannerClient, "spannerClient is null");
        this.identifierMapping = requireNonNull(identifierMapping, "identifierMapping is null");
    }

    public static @Nullable
    String toTrinoSchemaName(@Nullable String schema)
    {
        return "".equals(schema) ? DEFAULT_SCHEMA : schema;
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return spannerClient.getTableHandle(session, schemaTableName)
                .map(JdbcTableHandle::asPlainTable)
                .map(JdbcNamedRelationHandle::getRemoteTableName)
                .map(remoteTableName -> new JdbcTableHandle(
                        schemaTableName,
                        new RemoteTableName(remoteTableName.getCatalogName(),
                                Optional.ofNullable(toTrinoSchemaName(remoteTableName.getSchemaName().orElse(null))), remoteTableName.getTableName()),
                        Optional.empty()))
                .orElse(null);
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        List<LocalProperty<ColumnHandle>> sortingProperties = tableHandle.getSortOrder()
                .map(properties -> properties
                        .stream()
                        .map(item -> (LocalProperty<ColumnHandle>) new SortingProperty<ColumnHandle>(
                                item.getColumn(),
                                item.getSortOrder()))
                        .collect(toImmutableList()))
                .orElse(ImmutableList.of());

        return new ConnectorTableProperties(TupleDomain.all(), Optional.empty(), Optional.empty(), Optional.empty(), sortingProperties);
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
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        return new ConnectorTableMetadata(
                getSchemaTableName(handle),
                getColumnMetadata(session, handle),
                spannerClient.getTableProperties(session, handle));
    }

    private List<ColumnMetadata> getColumnMetadata(ConnectorSession session, JdbcTableHandle handle)
    {
        return spannerClient.getColumns(session, handle).stream()
                .map(JdbcColumnHandle::getColumnMetadata)
                .collect(toImmutableList());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        throw new TrinoException(NOT_SUPPORTED, "Can't create schemas in Spanner");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Can't drop schema which in spanner");
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        spannerClient.beginCreateTable(session, tableMetadata);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        return spannerClient.beginCreateTable(session, tableMetadata);
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
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;

        List<JdbcColumnHandle> columnHandles = columns.stream()
                .map(JdbcColumnHandle.class::cast)
                .collect(toImmutableList());

        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        return new JdbcOutputTableHandle(
                "spanner",
                remoteTableName.getSchemaName().orElse(null),
                remoteTableName.getTableName(),
                columnHandles.stream().map(JdbcColumnHandle::getColumnName).collect(toImmutableList()),
                columnHandles.stream().map(JdbcColumnHandle::getColumnType).collect(toImmutableList()),
                Optional.of(columnHandles.stream().map(JdbcColumnHandle::getJdbcTypeHandle).collect(toImmutableList())),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        if (column.getComment() != null) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with comments");
        }

        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        spannerClient.execute(session, format(
                "ALTER TABLE %s ADD %s %s",
                remoteTableName.getTableName(),
                spannerClient.quoted(column.getName()),
                spannerClient.toWriteMapping(session, column.getType()).getDataType()));
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        spannerClient.execute(session, format(
                "ALTER TABLE %s DROP COLUMN %s",
                remoteTableName.getTableName(),
                spannerClient.quoted(columnHandle.getColumnName())));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        spannerClient.dropTable(session, (JdbcTableHandle) tableHandle);
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
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
