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
package io.trino.plugin.hsqldb;

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
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;
import org.hsqldb.jdbcDriver;

import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.hsqldb.HsqlDbConfig.HSQLDB_DEFAULT_TABLE_TYPE;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;

public class HsqlDbClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(HsqlDbClient.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(HsqlDbMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(HsqlDbConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(HsqlDbJdbcConfig.class);
        configBinder(binder).bindConfig(HsqlDbConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        bindTablePropertiesProvider(binder, HsqlDbTableProperties.class);
        binder.bind(HsqlDbColumnProperties.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, HsqlDbConfig hsqldbConfig, OpenTelemetry openTelemetry)
    {
        return DriverConnectionFactory.builder(new jdbcDriver(), config.getConnectionUrl(), credentialProvider)
                .setConnectionProperties(getConnectionProperties(hsqldbConfig))
                .setOpenTelemetry(openTelemetry)
                .build();
    }

    private static Properties getConnectionProperties(HsqlDbConfig hsqldbConfig)
    {
        // XXX: Sets the type of table created when the CREATE TABLE statement is executed without
        // XXX: specifying a table type. The default type is MEMORY in HsqlDB but CACHED in Trino.
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(HSQLDB_DEFAULT_TABLE_TYPE, hsqldbConfig.getDefaultTableType().name());
        return connectionProperties;
    }
}
