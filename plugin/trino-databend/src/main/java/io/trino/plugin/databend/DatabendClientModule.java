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
package io.trino.plugin.databend;

import com.databend.jdbc.DatabendDriver;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalConfig;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;

public final class DatabendClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(DatabendClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        configBinder(binder).bindConfig(DatabendConfig.class);
        configBinder(binder).bindConfig(DecimalConfig.class);
        bindSessionPropertiesProvider(binder, DatabendSessionProperties.class);
        bindTablePropertiesProvider(binder, DatabendTableProperties.class);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory connectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, OpenTelemetry openTelemetry)
    {
        Properties connectionProperties = new Properties();
        return DriverConnectionFactory.builder(
                        new DatabendDriver(),
                        config.getConnectionUrl(),
                        credentialProvider)
                .setConnectionProperties(connectionProperties)
                .setOpenTelemetry(openTelemetry)
                .build();
    }
}
