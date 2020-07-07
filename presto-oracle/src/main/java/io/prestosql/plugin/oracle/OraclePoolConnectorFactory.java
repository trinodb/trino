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
package io.prestosql.plugin.oracle;

import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;

public class OraclePoolConnectorFactory
        implements ConnectionFactory
{
    private final PoolDataSource dataSource;
    private final CredentialProvider credentialProvider;

    public OraclePoolConnectorFactory(
            String connectionUrl,
            Properties connectionProperties,
            CredentialProvider credentialProvider,
            int connectionPoolMinSize,
            int connectionPoolMaxSize)
            throws SQLException
    {
        this.credentialProvider = credentialProvider;
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
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        Optional<String> user = credentialProvider.getConnectionUser(Optional.of(identity));
        Optional<String> password = credentialProvider.getConnectionPassword(Optional.of(identity));

        checkArgument(user.isPresent(), "Credentials returned null user");
        checkArgument(password.isPresent(), "Credentials returned null password");

        return dataSource.getConnection(user.get(), password.get());
    }
}
