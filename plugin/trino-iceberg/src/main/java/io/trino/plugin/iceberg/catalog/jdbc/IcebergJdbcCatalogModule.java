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
package io.trino.plugin.iceberg.catalog.jdbc;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;

import java.sql.Driver;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergJdbcCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergJdbcCatalogConfig.class);
        binder.bind(IcebergTableOperationsProvider.class).to(IcebergJdbcTableOperationsProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IcebergTableOperationsProvider.class).withGeneratedName();
        binder.bind(TrinoCatalogFactory.class).to(TrinoJdbcCatalogFactory.class).in(Scopes.SINGLETON);
        binder.bind(TrinoJdbcCatalogFactory.class);
        newExporter(binder).export(TrinoJdbcCatalogFactory.class).withGeneratedName();
    }

    @Provides
    @Singleton
    public static IcebergJdbcClient createIcebergJdbcClient(IcebergJdbcCatalogConfig config)
    {
        String driverClassName = requireNonNull(config.getDriverClass(), "driver class not configured");

        Class<? extends Driver> driverClass;
        try {
            driverClass = Class.forName(driverClassName).asSubclass(Driver.class);
        }
        catch (ClassNotFoundException | ClassCastException e) {
            throw new RuntimeException("Failed to load Driver class: " + driverClassName, e);
        }

        Driver driver;
        try {
            driver = driverClass.getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create instance of Driver: " + driverClassName, e);
        }
        return new IcebergJdbcClient(
                new IcebergJdbcConnectionFactory(driver, config.getConnectionUrl(), config.getConnectionUser(), config.getConnectionPassword()),
                config.getCatalogName());
    }
}
