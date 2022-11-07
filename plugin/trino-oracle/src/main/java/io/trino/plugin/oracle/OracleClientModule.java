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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.RetryingConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.ptf.ConnectorTableFunction;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleDriver;

import java.sql.SQLException;
import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.oracle.OracleClient.ORACLE_MAX_LIST_EXPRESSIONS;

public class OracleClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(OracleClient.class).in(Scopes.SINGLETON);
        bindSessionPropertiesProvider(binder, OracleSessionProperties.class);
        configBinder(binder).bindConfig(OracleConfig.class);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(ORACLE_MAX_LIST_EXPRESSIONS);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory connectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, OracleConfig oracleConfig)
            throws SQLException
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(OracleConnection.CONNECTION_PROPERTY_INCLUDE_SYNONYMS, String.valueOf(oracleConfig.isSynonymsEnabled()));
        connectionProperties.setProperty(OracleConnection.CONNECTION_PROPERTY_REPORT_REMARKS, String.valueOf(oracleConfig.isRemarksReportingEnabled()));

        if (oracleConfig.isConnectionPoolEnabled()) {
            return new OraclePoolConnectionFactory(
                    config.getConnectionUrl(),
                    connectionProperties,
                    credentialProvider,
                    oracleConfig.getConnectionPoolMinSize(),
                    oracleConfig.getConnectionPoolMaxSize(),
                    oracleConfig.getInactiveConnectionTimeout());
        }

        return new RetryingConnectionFactory(new DriverConnectionFactory(
                new OracleDriver(),
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider));
    }
}
