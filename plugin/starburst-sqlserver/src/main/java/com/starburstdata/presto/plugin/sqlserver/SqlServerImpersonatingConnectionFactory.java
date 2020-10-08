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

import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.plugin.jdbc.PreparingConnectionFactory;
import com.starburstdata.presto.plugin.jdbc.auth.ForAuthentication;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocal;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.starburstdata.presto.license.StarburstPrestoFeature.JDBC_IMPERSONATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SqlServerImpersonatingConnectionFactory
        extends PreparingConnectionFactory
{
    private final AuthToLocal authToLocal;

    @Inject
    public SqlServerImpersonatingConnectionFactory(LicenseManager licenseManager, @ForAuthentication ConnectionFactory delegate, AuthToLocal authToLocal)
    {
        super(delegate);
        licenseManager.checkFeature(JDBC_IMPERSONATION);
        this.authToLocal = requireNonNull(authToLocal, "authToLocal is null");
    }

    @Override
    protected void prepare(Connection connection, JdbcIdentity identity)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            statement.execute(format("EXECUTE AS USER = '%s'", authToLocal.translate(identity).replace("'", "''")));
        }
    }
}
