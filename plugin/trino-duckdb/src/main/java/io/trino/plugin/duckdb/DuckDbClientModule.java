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
package io.trino.plugin.duckdb;

import com.google.common.collect.ImmutableMap;
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
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.function.table.ConnectorTableFunction;
import org.duckdb.DuckDBDriver;

import java.util.Map;
import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.duckdb.DuckDBDriver.DUCKDB_USER_AGENT_PROPERTY;

public final class DuckDbClientModule
        extends AbstractConfigurationAwareModule
{
    // Keep the embedded engine from reaching the host file system, the network and extensions,
    // and stop SQL from turning these settings back on
    static final Map<String, String> SECURITY_CONNECTION_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("enable_external_access", "false")
            .put("allow_community_extensions", "false")
            .put("allow_unsigned_extensions", "false")
            .put("autoload_known_extensions", "false")
            .put("autoinstall_known_extensions", "false")
            .put("lock_configuration", "true")
            .buildOrThrow();

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(DuckDbClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DuckDbJdbcConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        configBinder(binder).bindConfig(DuckDbConfig.class);
        if (buildConfigObject(DuckDbConfig.class).isQueryPassthroughEnabled()) {
            newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
        }
        else {
            newOptionalBinder(binder, ConnectorAccessControl.class).setBinding().to(DuckDbAccessControl.class).in(Scopes.SINGLETON);
        }
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory connectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            OpenTelemetry openTelemetry,
            NodeVersion nodeVersion)
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(DUCKDB_USER_AGENT_PROPERTY, "trino/" + nodeVersion.toString());
        connectionProperties.putAll(SECURITY_CONNECTION_PROPERTIES);
        return new DuplicatingConnectionFactory(credentialProvider, DriverConnectionFactory.builder(
                        new DuckDBDriver(),
                        config.getConnectionUrl(),
                        credentialProvider)
                .setConnectionProperties(connectionProperties)
                .setOpenTelemetry(openTelemetry)
                .build());
    }
}
