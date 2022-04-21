/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.plugin.sqlserver.SqlServerClient;
import io.trino.plugin.sqlserver.SqlServerConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.TableScanRedirectApplicationResult;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static com.starburstdata.presto.plugin.sqlserver.StarburstCommonSqlServerSessionProperties.isBulkCopyForWrite;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerSessionProperties.isBulkCopyForWriteLockDestinationTable;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class StarburstSqlServerClient
        extends SqlServerClient
{
    private final TableScanRedirection tableScanRedirection;

    @Inject
    public StarburstSqlServerClient(
            BaseJdbcConfig config,
            SqlServerConfig sqlServerConfig,
            JdbcStatisticsConfig statisticsConfig,
            TableScanRedirection tableScanRedirection,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping)
    {
        super(config, sqlServerConfig, statisticsConfig, connectionFactory, queryBuilder, identifierMapping);
        this.tableScanRedirection = requireNonNull(tableScanRedirection, "tableScanRedirection is null");
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        JdbcOutputTableHandle table = super.beginCreateTable(session, tableMetadata);
        enableTableLockOnBulkLoadTableOption(session, table);
        return table;
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        JdbcOutputTableHandle table = super.beginInsertTable(session, tableHandle, columns);
        enableTableLockOnBulkLoadTableOption(session, table);
        return table;
    }

    private void enableTableLockOnBulkLoadTableOption(ConnectorSession session, JdbcOutputTableHandle table)
    {
        if (!isTableLockNeeded(session)) {
            return;
        }
        try (Connection connection = connectionFactory.openConnection(session)) {
            // 'table lock on bulk load' table option causes the bulk load processes on user-defined tables to obtain a bulk update lock
            // note: this is not a request to lock a table immediately
            String sql = format("EXEC sp_tableoption '%s', 'table lock on bulk load', '1'",
                    quoted(table.getCatalogName(), table.getSchemaName(), table.getTemporaryTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected String buildInsertSql(ConnectorSession session, RemoteTableName targetTable, RemoteTableName sourceTable, List<String> columnNames)
    {
        String columns = columnNames.stream()
                .map(this::quoted)
                .collect(joining(", "));
        return format("INSERT INTO %s %s (%s) SELECT %s FROM %s",
                targetTable,
                isTableLockNeeded(session) ? "WITH (TABLOCK)" : "", // TABLOCK is a prerequisite for minimal logging in SQL Server
                columns,
                columns,
                sourceTable);
    }

    /**
     * Table lock is a prerequisite for `minimal logging` in SQL Server
     *
     * @see <a href="https://docs.microsoft.com/en-us/sql/relational-databases/import-export/prerequisites-for-minimal-logging-in-bulk-import">minimal logging</a>
     */
    protected boolean isTableLockNeeded(ConnectorSession session)
    {
        return isBulkCopyForWrite(session) && isBulkCopyForWriteLockDestinationTable(session);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle handle)
    {
        return tableScanRedirection.getTableScanRedirection(session, handle, this);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcOutputTableHandle handle)
            throws SQLException
    {
        Connection connection = super.getConnection(session, handle);
        try {
            connection.unwrap(SQLServerConnection.class)
                    .setUseBulkCopyForBatchInsert(isBulkCopyForWrite(session));
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }
}
