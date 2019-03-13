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
package io.prestosql.plugin.mysql;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.mysql.cj.conf.ConnectionUrlParser;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.JdbcClient;

import static com.google.common.base.Preconditions.checkArgument;
import static com.mysql.cj.conf.ConnectionUrlParser.isConnectionStringSupported;
import static com.mysql.cj.conf.ConnectionUrlParser.parseConnectionString;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class MySqlClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).to(MySqlClient.class).in(Scopes.SINGLETON);
        ensureCatalogIsEmpty(buildConfigObject(BaseJdbcConfig.class).getConnectionUrl());
        configBinder(binder).bindConfig(MySqlConfig.class);
    }

    private static void ensureCatalogIsEmpty(String connectionUrl)
    {
        checkArgument(isConnectionStringSupported(connectionUrl), "Invalid JDBC URL for MySQL connector");
        ConnectionUrlParser parser = parseConnectionString(connectionUrl);
        checkArgument(parser.getPath().isEmpty(), "Database (catalog) must not be specified in JDBC URL for MySQL connector");
    }
}
