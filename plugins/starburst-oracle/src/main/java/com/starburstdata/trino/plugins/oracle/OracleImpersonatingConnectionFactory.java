/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import com.google.inject.Inject;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.plugin.jdbc.PreparingConnectionFactory;
import com.starburstdata.presto.plugin.jdbc.auth.ForImpersonation;
import com.starburstdata.presto.plugin.toolkit.authtolocal.AuthToLocal;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import oracle.jdbc.OracleConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class OracleImpersonatingConnectionFactory
        extends PreparingConnectionFactory
{
    private final AuthToLocal authToLocal;

    @Inject
    public OracleImpersonatingConnectionFactory(LicenseManager licenseManager, @ForImpersonation ConnectionFactory connectionFactory, AuthToLocal authToLocal)
    {
        super(connectionFactory);
        licenseManager.checkLicense();
        this.authToLocal = requireNonNull(authToLocal, "authToLocal is null");
    }

    @Override
    protected void prepare(Connection connection, ConnectorSession session)
            throws SQLException
    {
        OracleConnection oracleConnection = (OracleConnection) connection;
        Properties properties = new Properties();
        ConnectorIdentity identity = session.getIdentity();
        properties.setProperty(OracleConnection.PROXY_USER_NAME, authToLocal.translate(identity));
        // when working a pooled connection, close() will simply return it to the pool without
        // closing the proxy session; we guard against that condition by making sure that any
        // existing proxy session is closed before returning the connection to callers
        if (oracleConnection.isProxySession()) {
            oracleConnection.close(OracleConnection.PROXY_SESSION);
        }
        oracleConnection.openProxySession(OracleConnection.PROXYTYPE_USER_NAME, properties);
    }
}
