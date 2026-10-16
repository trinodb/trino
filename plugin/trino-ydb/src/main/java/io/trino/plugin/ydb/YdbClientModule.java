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
package io.trino.plugin.ydb;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.security.AllowAllAccessControl;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaRoutineName;
import tech.ydb.jdbc.YdbDriver;

import java.util.Properties;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class YdbClientModule
        extends AbstractModule
{
    @Override
    protected void configure()
    {
        newOptionalBinder(binder(), ConnectorAccessControl.class)
                .setBinding()
                .toInstance(new AllowAllAccessControl()
                {
                    @Override
                    public void checkCanExecuteProcedure(ConnectorSecurityContext context, SchemaRoutineName procedure)
                    {
                        if (procedure.equals(new SchemaRoutineName("system", "execute"))) {
                            throw new TrinoException(NOT_SUPPORTED, "This connector is read-only and does not support native execution");
                        }
                    }
                });
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public JdbcClient provideJdbcClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        return new YdbClient(config, connectionFactory, queryBuilder, identifierMapping, remoteQueryModifier);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider)
    {
        // JDBC 2.4.1 can close a cached context between lookup and connection registration.
        Properties properties = new Properties();
        properties.setProperty("cacheConnectionsInDriver", "false");
        // InListJdbcPrm bypasses the driver's native SDK-value binding path.
        properties.setProperty("replaceJdbcInByYqlList", "false");
        return DriverConnectionFactory.builder(
                        new YdbDriver(),
                        config.getConnectionUrl(),
                        credentialProvider)
                .setConnectionProperties(properties)
                .build();
    }
}
