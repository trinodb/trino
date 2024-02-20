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
package io.trino.plugin.redshift;

import com.amazon.redshift.Driver;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
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

import java.util.Properties;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class RedshiftClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(RedshiftConfig.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(RedshiftClient.class).in(SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(SINGLETON);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
    }

    @Singleton
    @Provides
    @ForBaseJdbc
    public static ConnectionFactory getConnectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            OpenTelemetry openTelemetry)
    {
        Properties properties = new Properties();
        properties.put("reWriteBatchedInserts", "true");
        properties.put("reWriteBatchedInsertsSize", "512");

        return new DriverConnectionFactory(new Driver(), config.getConnectionUrl(), properties, credentialProvider, openTelemetry);
    }
}
