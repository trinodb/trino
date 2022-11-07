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
package io.trino.plugin.raptor.legacy.metadata;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.raptor.legacy.util.DaoSupplier;
import org.jdbi.v3.core.ConnectionFactory;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Singleton;

import java.sql.DriverManager;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;

public class DatabaseMetadataModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder ignored)
    {
        install(conditionalModule(
                DatabaseConfig.class,
                config -> "mysql".equals(config.getDatabaseType()),
                new MySqlModule()));

        install(conditionalModule(
                DatabaseConfig.class,
                config -> "h2".equals(config.getDatabaseType()),
                new H2Module()));
    }

    private static class MySqlModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(JdbcDatabaseConfig.class);
        }

        @Provides
        @Singleton
        @ForMetadata
        public static ConnectionFactory createConnectionFactory(JdbcDatabaseConfig config)
        {
            String url = config.getUrl();
            return () -> DriverManager.getConnection(url);
        }

        @Provides
        @Singleton
        public static DaoSupplier<ShardDao> createShardDaoSupplier(@ForMetadata Jdbi dbi)
        {
            return new DaoSupplier<>(dbi, MySqlShardDao.class);
        }
    }

    private static class H2Module
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(H2DatabaseConfig.class);
        }

        @Provides
        @Singleton
        @ForMetadata
        public static ConnectionFactory createConnectionFactory(H2DatabaseConfig config)
        {
            String url = format("jdbc:h2:%s;DB_CLOSE_DELAY=-1", config.getFilename());
            return () -> DriverManager.getConnection(url);
        }

        @Provides
        @Singleton
        public static DaoSupplier<ShardDao> createShardDaoSupplier(@ForMetadata Jdbi dbi)
        {
            return new DaoSupplier<>(dbi, H2ShardDao.class);
        }
    }
}
