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

import io.prestosql.plugin.jdbc.AuthToLocal;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.PreparingConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SnowflakeImpersonationConnectionFactory
        extends PreparingConnectionFactory
{
    private final AuthToLocal authToLocal;

    SnowflakeImpersonationConnectionFactory(ConnectionFactory connectionFactory, AuthToLocal authToLocal)
    {
        super(connectionFactory);
        this.authToLocal = requireNonNull(authToLocal, "authToLocal is null");
    }

    @Override
    protected void prepare(Connection connection, JdbcIdentity identity)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            statement.execute(format("USE ROLE %s", authToLocal.translate(identity)));
        }
    }
}
