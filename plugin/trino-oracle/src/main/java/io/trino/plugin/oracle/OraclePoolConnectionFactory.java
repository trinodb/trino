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
package io.trino.plugin.oracle;

import io.airlift.units.Duration;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.ConnectorSession;
import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OraclePoolConnectionFactory
        implements ConnectionFactory
{
    private final PoolDataSource dataSource;

    public OraclePoolConnectionFactory(
            String connectionUrl,
            Properties connectionProperties,
            CredentialProvider credentialProvider,
            int connectionPoolMinSize,
            int connectionPoolMaxSize,
            Duration inactiveConnectionTimeout)
            throws SQLException
    {
        this.dataSource = PoolDataSourceFactory.getPoolDataSource();

        //Setting connection properties of the data source
        this.dataSource.setConnectionFactoryClassName(OracleDataSource.class.getName());
        this.dataSource.setURL(connectionUrl);

        //Setting pool properties
        this.dataSource.setInitialPoolSize(connectionPoolMinSize);
        this.dataSource.setMinPoolSize(connectionPoolMinSize);
        this.dataSource.setMaxPoolSize(connectionPoolMaxSize);
        this.dataSource.setValidateConnectionOnBorrow(true);
        this.dataSource.setConnectionProperties(connectionProperties);
        this.dataSource.setInactiveConnectionTimeout(toIntExact(inactiveConnectionTimeout.roundTo(SECONDS)));
        credentialProvider.getConnectionUser(Optional.empty())
                .ifPresent(user -> {
                    try {
                        dataSource.setUser(user);
                    }
                    catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
        credentialProvider.getConnectionPassword(Optional.empty())
                .ifPresent(password -> {
                    try {
                        dataSource.setPassword(password);
                    }
                    catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Connection connection = dataSource.getConnection();
        // Oracle's pool doesn't reset autocommit state of connections when reusing them so we explicitly enable
        // autocommit by default to match the JDBC specification.
        connection.setAutoCommit(true);
        return connection;
    }
}
