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
package io.trino.plugin.firebird;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;

public class FirebirdClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(FirebirdClient.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(FirebirdMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(FirebirdConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(FirebirdJdbcConfig.class);
        configBinder(binder).bindConfig(FirebirdConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        bindTablePropertiesProvider(binder, FirebirdTableProperties.class);
        binder.bind(FirebirdColumnProperties.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }
}
