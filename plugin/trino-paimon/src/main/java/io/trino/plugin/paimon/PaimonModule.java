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
package io.trino.plugin.paimon;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HideDeltaLakeTables;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class PaimonModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(PaimonConfig.class);
        configBinder(binder).bindConfig(OrcReaderConfig.class);
        configBinder(binder).bindConfig(ParquetReaderConfig.class);

        binder.bind(PaimonMetadataFactory.class).in(SINGLETON);
        binder.bind(PaimonSplitManager.class).in(SINGLETON);
        binder.bind(PaimonPageSourceProvider.class).in(SINGLETON);
        binder.bind(PaimonSessionProperties.class).in(SINGLETON);
        binder.bind(PaimonTableOptions.class).in(SINGLETON);
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        binder.bind(PaimonTransactionManager.class).in(SINGLETON);
//        binder.bind(PaimonTrinoCatalogFactory.class).in(SINGLETON);
        binder.bind(ConnectorSplitManager.class)
                .annotatedWith(ForClassLoaderSafe.class)
                .to(PaimonSplitManager.class)
                .in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class)
                .to(ClassLoaderSafeConnectorSplitManager.class)
                .in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class)
                .annotatedWith(ForClassLoaderSafe.class)
                .to(PaimonPageSourceProvider.class)
                .in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class)
                .to(ClassLoaderSafeConnectorPageSourceProvider.class)
                .in(Scopes.SINGLETON);

        binder.bind(PaimonConnector.class).in(Scopes.SINGLETON);

        binder.bind(boolean.class).annotatedWith(HideDeltaLakeTables.class).toInstance(false);
        newOptionalBinder(binder, Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class));
    }
}
