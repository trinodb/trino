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
package io.trino.plugin.ydb;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;

public class TestingYdbJdbcModule
        extends AbstractModule
{
    @Override
    protected void configure()
    {
        install(Modules.override(new YdbClientModule()).with(new AbstractModule()
        {
            @Provides
            @Singleton
            @ForBaseJdbc
            public JdbcClient provideJdbcClient(
                    BaseJdbcConfig config,
                    ConnectionFactory connectionFactory,
                    QueryBuilder queryBuilder,
                    IdentifierMapping identifierMapping,
                    RemoteQueryModifier remoteQueryModifier)
            {
                return new TestingYdbJdbcClient(config, connectionFactory, queryBuilder, identifierMapping, remoteQueryModifier);
            }
        }));
    }
}
