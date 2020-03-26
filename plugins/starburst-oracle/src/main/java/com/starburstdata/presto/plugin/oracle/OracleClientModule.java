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
package com.starburstdata.presto.plugin.oracle;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.BaseJdbcStatisticsConfig;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.SessionPropertiesProvider;
import io.prestosql.plugin.jdbc.caching.BaseJdbcCachingConfig;
import io.prestosql.spi.procedure.Procedure;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class OracleClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).to(OracleClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(BaseJdbcCachingConfig.class);
        configBinder(binder).bindConfig(BaseJdbcStatisticsConfig.class);
        configBinder(binder).bindConfig(OracleConfig.class);
        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(AnalyzeProcedure.class).in(Scopes.SINGLETON);

        Multibinder<SessionPropertiesProvider> sessionProperties = newSetBinder(binder, SessionPropertiesProvider.class);
        sessionProperties.addBinding().to(OracleSessionProperties.class);
    }
}
