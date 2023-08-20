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
package io.trino.plugin.clickhouse;

import com.clickhouse.jdbc.ClickHouseDriver;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.ConfigBinder;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import java.util.Properties;

import static com.clickhouse.client.config.ClickHouseClientOption.USE_BINARY_STRING;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.clickhouse.ClickHouseClient.DEFAULT_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;

public class ClickHouseClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        ConfigBinder.configBinder(binder).bindConfig(ClickHouseConfig.class);
        bindSessionPropertiesProvider(binder, ClickHouseSessionProperties.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(ClickHouseClient.class).in(Scopes.SINGLETON);
        bindTablePropertiesProvider(binder, ClickHouseTableProperties.class);
        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setDomainCompactionThreshold(DEFAULT_DOMAIN_COMPACTION_THRESHOLD));
        binder.install(new DecimalModule());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, OpenTelemetry openTelemetry)
    {
        Properties properties = new Properties();
        // The connector expects byte array for FixedString and String types
        properties.setProperty(USE_BINARY_STRING.getKey(), "true");
        return new ClickHouseConnectionFactory(new DriverConnectionFactory(new ClickHouseDriver(), config.getConnectionUrl(), properties, credentialProvider, openTelemetry));
    }
}
