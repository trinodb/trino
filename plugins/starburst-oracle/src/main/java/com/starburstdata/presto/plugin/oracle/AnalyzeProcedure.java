/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.procedure.Procedure;

import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class AnalyzeProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle ANALYZE = methodHandle(
            AnalyzeProcedure.class,
            "analyze",
            ConnectorSession.class,
            String.class,
            String.class);

    private final JdbcClient client;
    private final ConnectionFactory connectionFactory;

    @Inject
    public AnalyzeProcedure(JdbcClient client, ConnectionFactory connectionFactory)
    {
        this.client = requireNonNull(client, "client is null");
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "analyze",
                ImmutableList.of(
                        new Procedure.Argument("schema_name", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR)),
                ANALYZE.bindTo(this));
    }

    public void analyze(ConnectorSession session, String schemaName, String tableName)
            throws PrestoException
    {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        Optional<JdbcTableHandle> tableHandle = client.getTableHandle(session, schemaTableName);
        if (tableHandle.isEmpty()) {
            throw new TableNotFoundException(schemaTableName);
        }

        try (Connection connection = connectionFactory.openConnection(session);
                CallableStatement statement = connection.prepareCall("{CALL DBMS_STATS.GATHER_TABLE_STATS(?, ?)}")) {
            statement.setString(1, schemaName);
            statement.setString(2, tableName);
            statement.execute();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
}
