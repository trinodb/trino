/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.jdbc;

import com.starburstdata.presto.plugin.jdbc.PreparingConnectionFactory;
import com.starburstdata.presto.plugin.jdbc.auth.ForImpersonation;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocal;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcIdentity;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SnowflakeImpersonationConnectionFactory
        extends PreparingConnectionFactory
{
    private final AuthToLocal authToLocal;

    @Inject
    public SnowflakeImpersonationConnectionFactory(@ForImpersonation ConnectionFactory connectionFactory, AuthToLocal authToLocal)
    {
        super(connectionFactory);
        this.authToLocal = requireNonNull(authToLocal, "authToLocal is null");
    }

    @Override
    protected void prepare(Connection connection, ConnectorSession session)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            JdbcIdentity identity = JdbcIdentity.from(session);
            statement.execute(format("USE ROLE %s", authToLocal.translate(identity)));
        }
    }
}
