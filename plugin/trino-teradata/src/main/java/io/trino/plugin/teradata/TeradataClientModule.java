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
package io.trino.plugin.teradata;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class TeradataClientModule
        extends AbstractConfigurationAwareModule
{
    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory getConnectionFactory(BaseJdbcConfig config, TeradataConfig teradataConfig, CredentialProvider credentialProvider, OpenTelemetry openTelemetry)
            throws SQLException
    {
        Properties connectionProperties = new Properties();
        Driver driver = DriverManager.getDriver(config.getConnectionUrl());
        String longMech = LogonMechanism.fromString(teradataConfig.getLogMech()).getMechanism();
        connectionProperties.put("LOGMECH", longMech);
        String clientId;
        switch (longMech) {
            case "TD2":
                break;
            default:
                throw new IllegalArgumentException("Unsupported logon mechanism: " + longMech);
        }
        return DriverConnectionFactory.builder(driver, config.getConnectionUrl(), credentialProvider).setConnectionProperties(connectionProperties).setOpenTelemetry(openTelemetry).build();
    }

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(TeradataConfig.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(TeradataClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        install(new JdbcJoinPushdownSupportModule());
    }
}
