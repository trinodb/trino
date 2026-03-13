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
package io.trino.plugin.ducklake;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProviderFactory;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.SqliteDucklakeCatalog;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSplitManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

/**
 * Guice module for Ducklake connector bindings.
 */
public class DucklakeModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        // Configuration
        configBinder(binder).bindConfig(DucklakeConfig.class);
        configBinder(binder).bindConfig(ParquetReaderConfig.class);

        // Core connector components
        binder.bind(DucklakeConnector.class).in(Scopes.SINGLETON);
        binder.bind(DucklakeTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(DucklakeMetadataFactory.class).in(Scopes.SINGLETON);

        // Catalog
        binder.bind(DucklakeCatalog.class).to(SqliteDucklakeCatalog.class).in(Scopes.SINGLETON);

        // File system factory
        binder.bind(DucklakeFileSystemFactory.class).to(DefaultDucklakeFileSystemFactory.class).in(Scopes.SINGLETON);

        // Split manager
        binder.bind(ConnectorSplitManager.class)
                .annotatedWith(ForClassLoaderSafe.class)
                .to(DucklakeSplitManager.class)
                .in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class)
                .to(ClassLoaderSafeConnectorSplitManager.class)
                .in(Scopes.SINGLETON);

        // Page source provider
        binder.bind(ConnectorPageSourceProviderFactory.class)
                .annotatedWith(ForClassLoaderSafe.class)
                .to(DucklakePageSourceProviderFactory.class)
                .in(Scopes.SINGLETON);
        binder.bind(DucklakePageSourceProviderFactory.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProviderFactory.class)
                .to(ClassLoaderSafeConnectorPageSourceProviderFactory.class)
                .in(Scopes.SINGLETON);

        // Parquet reader configuration and stats
        // Note: ParquetReaderOptions is created in the factory from ParquetReaderConfig
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        // Type converter
        binder.bind(DucklakeTypeConverter.class).in(Scopes.SINGLETON);
    }
}
