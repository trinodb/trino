/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.jdbc;

import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public abstract class PreparingConnectionFactory
        implements ConnectionFactory
{
    private final ConnectionFactory delegate;

    protected PreparingConnectionFactory(ConnectionFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Connection connection = delegate.openConnection(session);
        try {
            prepare(connection, session);
        }
        catch (SQLException e) {
            try (Connection ignored = connection) {
                throw e;
            }
        }
        return connection;
    }

    protected abstract void prepare(Connection connection, ConnectorSession session)
            throws SQLException;

    @Override
    public void close()
            throws SQLException
    {
        delegate.close();
    }
}
