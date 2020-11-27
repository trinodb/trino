/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

public class PrestoConnectorClient
        extends BaseJdbcClient
{
    private final boolean enableWrites;

    @Inject
    public PrestoConnectorClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, @EnableWrites boolean enableWrites)
    {
        super(config, "\"", connectionFactory);
        this.enableWrites = enableWrites;
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        String sql = format(
                "ALTER TABLE %s ADD COLUMN %s",
                quoted(handle.getRemoteTableName()),
                getColumnDefinitionSql(session, column, column.getName()));
        execute(JdbcIdentity.from(session), sql);
    }

    @Override
    public void setColumnComment(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        String sql = format(
                "COMMENT ON COLUMN %s.%s IS %s",
                quoted(handle.getRemoteTableName()),
                quoted(column.getColumnName()),
                comment.isPresent() ? format("'%s'", comment.get()) : "NULL");
        execute(identity, sql);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support inserts");
        }
        return super.beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables");
        }
        super.createTable(session, tableMetadata);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
        }
        return super.beginCreateTable(session, tableMetadata);
    }

    @Override
    public void dropTable(JdbcIdentity identity, JdbcTableHandle handle)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping tables");
        }
        super.dropTable(identity, handle);
    }

    @Override
    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming tables");
        }
        super.renameTable(identity, handle, newTableName);
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming columns");
        }
        super.renameColumn(identity, handle, jdbcColumn, newColumnName);
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping columns");
        }
        super.dropColumn(identity, handle, column);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }
}
