/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class PasswordAuthenticationConnectionProvider
        implements OracleConnectionProvider
{
    @Override
    public Connection getConnection(Optional<CredentialProvider> credentialProvider, ConnectorSession session, PoolDataSource dataSource)
            throws SQLException
    {
        verify(credentialProvider.isPresent(), "Credential provider is missing");
        Optional<ConnectorIdentity> identity = Optional.of(session.getIdentity());
        String username = credentialProvider.get().getConnectionUser(identity).orElse("");
        String password = credentialProvider.get().getConnectionPassword(identity).orElse("");

        if (username.isEmpty() || password.isEmpty()) {
            throw new TrinoException(GENERIC_USER_ERROR, "Password authentication mode requires user credentials");
        }

        return dataSource.getConnection(username, password);
    }

    @Override
    public void validateConnectionCredentials(PoolDataSource dataSource)
    {
        if (dataSource.getUser() == null || dataSource.getUser().isEmpty()) {
            throw new TrinoException(CONFIGURATION_INVALID, "Password mode requires connection-user to discover cluster topology");
        }
    }
}
