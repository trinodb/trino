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
package io.trino.plugin.snowflake;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.TypeHandlingJdbcConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.ConnectorTableFunction;
import net.snowflake.client.jdbc.SnowflakeDriver;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class SnowflakeClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SnowflakeClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(SnowflakeConfig.class);
        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(BaseJdbcConfig baseJdbcConfig, SnowflakeConfig snowflakeConfig, CredentialProvider credentialProvider, OpenTelemetry openTelemetry)
            throws MalformedURLException
    {
        Properties properties = new Properties();
        snowflakeConfig.getAccount().ifPresent(account -> properties.setProperty("account", account));
        snowflakeConfig.getDatabase().ifPresent(database -> properties.setProperty("db", database));
        snowflakeConfig.getRole().ifPresent(role -> properties.setProperty("role", role));
        snowflakeConfig.getWarehouse().ifPresent(warehouse -> properties.setProperty("warehouse", warehouse));

        // Set the expected date/time formatting we expect for our plugin to parse
        properties.setProperty("TIMESTAMP_OUTPUT_FORMAT", "YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM");
        properties.setProperty("TIMESTAMP_NTZ_OUTPUT_FORMAT", "YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM");
        properties.setProperty("TIMESTAMP_TZ_OUTPUT_FORMAT", "YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM");
        properties.setProperty("TIMESTAMP_LTZ_OUTPUT_FORMAT", "YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM");
        properties.setProperty("TIME_OUTPUT_FORMAT", "HH24:MI:SS.FF9");

        // Support for Corporate proxies
        if (snowflakeConfig.getHttpProxy().isPresent()) {
            String proxy = snowflakeConfig.getHttpProxy().get();

            URL url = new URL(proxy);

            properties.setProperty("useProxy", "true");
            properties.setProperty("proxyHost", url.getHost());
            properties.setProperty("proxyPort", Integer.toString(url.getPort()));
            properties.setProperty("proxyProtocol", url.getProtocol());

            String userInfo = url.getUserInfo();
            if (userInfo != null) {
                String[] usernamePassword = userInfo.split(":", 2);

                if (usernamePassword.length != 2) {
                    throw new TrinoException(NOT_SUPPORTED, "Improper snowflake.http_proxy. username:password@ is optional but what was entered was not correct");
                }

                properties.setProperty("proxyUser", usernamePassword[0]);
                properties.setProperty("proxyPassword", usernamePassword[1]);
            }
        }

        return new DriverConnectionFactory(new SnowflakeDriver(), baseJdbcConfig.getConnectionUrl(), properties, credentialProvider, openTelemetry);
    }
}
