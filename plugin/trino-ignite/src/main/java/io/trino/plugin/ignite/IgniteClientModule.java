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
package io.trino.plugin.ignite;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.internal.jdbc.thin.JdbcThinUtils;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;
import static java.util.Objects.requireNonNull;

public class IgniteClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(IgniteClient.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(IgniteJdbcMetadataFactory.class).in(Scopes.SINGLETON);
        bindTablePropertiesProvider(binder, IgniteTableProperties.class);
        binder.install(new DecimalModule());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        // https://github.com/trinodb/trino/issues/16148
        checkArgument(requireNonNull(config.getConnectionUrl()).startsWith(JdbcThinUtils.URL_PREFIX), "Invalid JDBC URL for Ignite connector, connection url should start with 'jdbc:ignite:thin://', actual get %s", config.getConnectionUrl());
        Properties connectionProperties = new Properties();
        return new DriverConnectionFactory(
                new IgniteJdbcThinDriver(),
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider);
    }
}
