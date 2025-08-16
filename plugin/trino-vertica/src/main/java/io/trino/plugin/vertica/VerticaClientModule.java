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
package io.trino.plugin.vertica;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConfiguringConnectionFactory;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.sql.Statement;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class VerticaClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(VerticaClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(VerticaClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        configBinder(binder).bindConfigDefaults(JdbcStatisticsConfig.class, config -> {
            // Disabled by default because the user must be superuser to run EXPORT_STATISTICS function in Vertica
            config.setEnabled(false);
        });

        binder.install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());

        @SuppressWarnings("TrinoExperimentalSpi")
        Class<ConnectorTableFunction> clazz = ConnectorTableFunction.class;
        newSetBinder(binder, clazz).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, OpenTelemetry openTelemetry)
    {
        ConnectionFactory connectionFactory = DriverConnectionFactory.builder(new VerticaDriver(), config.getConnectionUrl(), credentialProvider)
                .setOpenTelemetry(openTelemetry)
                .build();

        return new ConfiguringConnectionFactory(connectionFactory, connection -> {
            // In vertica all the underlying projections data stored according to en_US@collation=binary
            // So to be safe with pushdowns and guarantee the same results for trino with and without pushdowns
            // we should set session level collation to en_US@collation=binary,
            // in this case setting up database level collation (for all incoming sessions) will not break pushdown functionlaity
            try (Statement stmt = connection.createStatement()) {
                // This sets the session locale to match Vertica's internal collation
                stmt.execute("SET LOCALE TO 'en_US@collation=binary'");
            }
        });
    }
}
