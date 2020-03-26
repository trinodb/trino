/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.inject.Inject;
import io.prestosql.plugin.jdbc.AuthToLocal;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.PreparingConnectionFactory;
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
    public OracleImpersonatingConnectionFactory(@OracleAuthenticationModule.ForAuthentication ConnectionFactory connectionFactory, AuthToLocal authToLocal)
    {
        super(connectionFactory);
        this.authToLocal = requireNonNull(authToLocal, "authToLocal is null");
    }

    @Override
    protected void prepare(Connection connection, JdbcIdentity identity)
            throws SQLException
    {
        OracleConnection oracleConnection = (OracleConnection) connection;
        Properties properties = new Properties();
        properties.put(OracleConnection.PROXY_USER_NAME, authToLocal.translate(identity));
        // when working a pooled connection, close() will simply return it to the pool without
        // closing the proxy session; we guard against that condition by making sure that any
        // existing proxy session is closed before returning the connection to callers
        if (oracleConnection.isProxySession()) {
            oracleConnection.close(OracleConnection.PROXY_SESSION);
        }
        oracleConnection.openProxySession(OracleConnection.PROXYTYPE_USER_NAME, properties);
    }
}
