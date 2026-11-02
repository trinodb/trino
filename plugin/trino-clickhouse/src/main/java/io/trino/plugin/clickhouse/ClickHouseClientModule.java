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

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.jdbc.Driver;
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
import io.trino.plugin.jdbc.FloatingPointQueryBuilder;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.clickhouse.ClickHouseClient.DEFAULT_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.jdbc.DecimalModule.MappingToNumber.ON_BY_DEFAULT;
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
        newOptionalBinder(binder, QueryBuilder.class).setBinding().to(FloatingPointQueryBuilder.class).in(Scopes.SINGLETON);
        bindTablePropertiesProvider(binder, ClickHouseTableProperties.class);
        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setDomainCompactionThreshold(DEFAULT_DOMAIN_COMPACTION_THRESHOLD));
        binder.install(DecimalModule.withNumberMapping(ON_BY_DEFAULT));
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, OpenTelemetry openTelemetry)
    {
        Properties properties = new Properties();
        // The connector expects raw bytes for String and FixedString columns; without this,
        // clickhouse-jdbc decodes them as UTF-8 (replacing invalid sequences with U+FFFD) before
        // getBytes() re-encodes, corrupting non-UTF-8 varbinary data.
        properties.setProperty(ClientConfigProperties.BINARY_STRING_SUPPORT.getKey(), "true");
        return new ClickHouseConnectionFactory(DriverConnectionFactory.builder(new Driver(), config.getConnectionUrl(), credentialProvider)
                .setConnectionProperties(properties)
                .setOpenTelemetry(openTelemetry)
                .build());
    }
}
