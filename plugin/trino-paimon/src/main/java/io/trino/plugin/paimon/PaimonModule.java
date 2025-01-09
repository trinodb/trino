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
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.paimon.catalog.PaimonTrinoCatalogFactory;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Module for binding instance.
 */
public class PaimonModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(PaimonConfig.class);
        binder.bind(PaimonMetadataFactory.class).in(SINGLETON);
        binder.bind(PaimonSessionProperties.class).in(SINGLETON);
        binder.bind(PaimonTableOptions.class).in(SINGLETON);
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        binder.bind(PaimonTransactionManager.class).in(SINGLETON);
        binder.bind(PaimonTrinoCatalogFactory.class).in(SINGLETON);
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
    }
}
