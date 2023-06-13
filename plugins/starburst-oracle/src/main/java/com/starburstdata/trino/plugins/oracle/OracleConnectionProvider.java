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

import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.ConnectorSession;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

public interface OracleConnectionProvider
{
    void validateConnectionCredentials(PoolDataSource dataSource);

    Connection getConnection(Optional<CredentialProvider> credentialProvider, ConnectorSession session, PoolDataSource dataSource)
            throws SQLException;
}
