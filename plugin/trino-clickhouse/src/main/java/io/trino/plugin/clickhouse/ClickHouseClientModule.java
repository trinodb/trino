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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigBinder;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.Driver;

import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;
import static java.lang.String.format;

public class ClickHouseClientModule
        implements Module
{
    private static final String CLICKHOUSE_LATEST_DRIVER_CLASS_NAME = "com.clickhouse.jdbc.ClickHouseDriver";
    // TODO: This Driver will not be available when clickhouse-jdbc is upgraded to 0.4.0 or above
    private static final String CLICKHOUSE_DEPRECATED_DRIVER_CLASS_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

    @Override
    public void configure(Binder binder)
    {
        ConfigBinder.configBinder(binder).bindConfig(ClickHouseConfig.class);
        bindSessionPropertiesProvider(binder, ClickHouseSessionProperties.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(ClickHouseClient.class).in(Scopes.SINGLETON);
        bindTablePropertiesProvider(binder, ClickHouseTableProperties.class);
        binder.install(new DecimalModule());
        binder.bind(ConnectionFactory.class).annotatedWith(ForBaseJdbc.class).toProvider(ConnectionFactoryProvider.class).in(Scopes.SINGLETON);
    }

    private static class ConnectionFactoryProvider
            implements Provider<ConnectionFactory>
    {
        private final ConnectionFactory connectionFactory;

        @Inject
        public ConnectionFactoryProvider(ClickHouseConfig clickHouseConfig, BaseJdbcConfig baseJdbcConfig, CredentialProvider credentialProvider)
        {
            connectionFactory = new ClickHouseConnectionFactory(
                    new DriverConnectionFactory(createDriver(clickHouseConfig.isUseDeprecatedDriver()), baseJdbcConfig, credentialProvider));
        }

        @Override
        public ConnectionFactory get()
        {
            return connectionFactory;
        }

        Driver createDriver(boolean useDeprecatedDriver)
        {
            String driverClass = getDriverClassName(useDeprecatedDriver);
            try {
                return (Driver) Class.forName(driverClass, true, getClassLoader()).getDeclaredConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(format("Error creating an instance of %s", driverClass), e);
            }
        }

        String getDriverClassName(boolean useDeprecatedDriver)
        {
            if (useDeprecatedDriver) {
                return CLICKHOUSE_DEPRECATED_DRIVER_CLASS_NAME;
            }
            return CLICKHOUSE_LATEST_DRIVER_CLASS_NAME;
        }

        ClassLoader getClassLoader()
        {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader == null) {
                classLoader = ClickHouseConfig.class.getClassLoader();
            }
            return classLoader;
        }
    }
}
