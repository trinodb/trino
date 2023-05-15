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
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.CatalogManager;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class StaticCatalogManagerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(StaticCatalogManagerConfig.class);
        binder.bind(StaticCatalogManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorServicesProvider.class).to(StaticCatalogManager.class).in(Scopes.SINGLETON);
        binder.bind(CatalogManager.class).to(StaticCatalogManager.class).in(Scopes.SINGLETON);

        binder.bind(LazyRegister.class).asEagerSingleton();
    }

    private static class LazyRegister
    {
        @Inject
        public LazyRegister(
                DefaultCatalogFactory defaultCatalogFactory,
                LazyCatalogFactory lazyCatalogFactory,
                StaticCatalogManager catalogManager,
                GlobalSystemConnector globalSystemConnector)
        {
            lazyCatalogFactory.setCatalogFactory(defaultCatalogFactory);
            catalogManager.registerGlobalSystemConnector(globalSystemConnector);
        }
    }
}
