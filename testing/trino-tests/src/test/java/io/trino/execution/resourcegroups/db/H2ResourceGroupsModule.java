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
package io.trino.execution.resourcegroups.db;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.plugin.resourcegroups.db.DatabaseMigrator;
import io.trino.plugin.resourcegroups.db.DbResourceGroupConfig;
import io.trino.plugin.resourcegroups.db.DbResourceGroupConfigurationManager;
import io.trino.plugin.resourcegroups.db.H2DaoProvider;
import io.trino.plugin.resourcegroups.db.ResourceGroupsDao;
import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class H2ResourceGroupsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(DbResourceGroupConfig.class);
        binder.bind(ResourceGroupsDao.class).toProvider(H2DaoProvider.class).in(Scopes.SINGLETON);
        binder.bind(DbResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static DatabaseMigrator createMigrator()
    {
        // H2 tests set up schema manually; skip Flyway migrations for this non-production database.
        return () -> {};
    }

    @Provides
    @Singleton
    public static DataSource createDataSource(DbResourceGroupConfig config)
    {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(config.getConfigDbUrl());
        return ds;
    }
}
