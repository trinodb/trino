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
package io.prestosql.plugin.salesforce;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.TypeHandlingJdbcConfig;
import io.prestosql.plugin.jdbc.credential.CredentialConfig;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import io.prestosql.plugin.salesforce.driver.ForceDriver;
import io.prestosql.plugin.salesforce.driver.ForceModule;

import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class SalesforceClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.install(new ForceModule());

        binder.bind(JdbcClient.class)
                .annotatedWith(ForBaseJdbc.class)
                .to(SalesforceClient.class)
                .in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        configBinder(binder).bindConfig(SalesforceConfig.class);
    }

    @Singleton
    @Provides
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(
            BaseJdbcConfig baseConfig,
            CredentialConfig credentialConfig,
            SalesforceConfig salesforceConfig,
            CredentialProvider credentialProvider)
    {
        Properties properties = new Properties();

        salesforceConfig.getSecurityToken().ifPresent(
                token -> properties.setProperty("securityToken", token)
        );

        return new DriverConnectionFactory(new ForceDriver(), baseConfig.getConnectionUrl(), properties, credentialProvider);
    }
}
