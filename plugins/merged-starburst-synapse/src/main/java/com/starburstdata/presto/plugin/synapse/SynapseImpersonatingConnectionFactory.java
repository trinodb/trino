/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.plugin.jdbc.PreparingConnectionFactory;
import com.starburstdata.presto.plugin.jdbc.auth.ForImpersonation;
import com.starburstdata.presto.plugin.toolkit.authtolocal.AuthToLocal;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.starburstdata.presto.license.StarburstFeature.JDBC_IMPERSONATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SynapseImpersonatingConnectionFactory
        extends PreparingConnectionFactory
{
    private final AuthToLocal authToLocal;

    @Inject
    public SynapseImpersonatingConnectionFactory(LicenseManager licenseManager, @ForImpersonation ConnectionFactory delegate, AuthToLocal authToLocal)
    {
        super(delegate);
        licenseManager.checkFeature(JDBC_IMPERSONATION);
        this.authToLocal = requireNonNull(authToLocal, "authToLocal is null");
    }

    @Override
    protected void prepare(Connection connection, ConnectorSession session)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            statement.execute(format("EXECUTE AS USER = '%s'", authToLocal.translate(session.getIdentity()).replace("'", "''")));
        }
    }
}
