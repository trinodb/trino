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
package io.trino.plugin.sybase;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.sybase.jdbc4.jdbc.SybDriver;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.TypeHandlingJdbcConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class SybaseClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SybaseClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(SybaseConfig.class);
        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        binder.install(new DecimalModule());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, SybaseConfig sybaseConfig, OpenTelemetry openTelemetry)
    {
        Properties connectionProperties = new Properties();
        // https://www-01.ibm.com/support/knowledgecenter/ssw_ibm_i_72/rzaha/conprop.htm
        // block size (a.k.a fetch size), default 32
        connectionProperties.setProperty("block size", "512");

        // use IAM authentication when using API key
        if (sybaseConfig.getApiKey() != null) {
            connectionProperties.setProperty("apiKey", sybaseConfig.getApiKey());
            connectionProperties.setProperty("securityMechanism", "15");
            connectionProperties.setProperty("pluginName", "IBMIAMauth");
        }

        return DriverConnectionFactory.builder(new SybDriver(), config.getConnectionUrl(), credentialProvider)
                .setConnectionProperties(connectionProperties)
                .setOpenTelemetry(openTelemetry)
                .build();
    }
}
