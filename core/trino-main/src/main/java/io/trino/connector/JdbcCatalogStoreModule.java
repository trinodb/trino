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
package io.trino.connector;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.spi.catalog.CatalogStore;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class JdbcCatalogStoreModule
        implements Module {
    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(JdbcCatalogStoreConfig.class);
        binder.bind(CatalogStore.class).to(JdbcCatalogStore.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static Jdbi createJdbi(JdbcCatalogStoreConfig config) {
        return Jdbi.create(config.getUrl(), config.getUser(), config.getPassword());
    }

    @Provides
    @Singleton
    public static JdbcCatalogStoreDao createDao(Jdbi jdbi) {
        JdbcCatalogStoreDao dao = jdbi
                .installPlugin(new SqlObjectPlugin())
                .onDemand(JdbcCatalogStoreDao.class);
        dao.createCatalogsTable();
        return dao;
    }
}
