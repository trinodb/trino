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
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.CatalogManager;
import io.trino.server.ServerConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class DynamicCatalogManagerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        if (buildConfigObject(ServerConfig.class).isCoordinator()) {
            binder.bind(CoordinatorDynamicCatalogManager.class).in(Scopes.SINGLETON);
            CatalogStoreConfig config = buildConfigObject(CatalogStoreConfig.class);
            switch (config.getCatalogStoreKind()) {
                case MEMORY -> binder.bind(CatalogStore.class).to(InMemoryCatalogStore.class).in(Scopes.SINGLETON);
                case FILE -> {
                    configBinder(binder).bindConfig(FileCatalogStoreConfig.class);
                    binder.bind(CatalogStore.class).to(FileCatalogStore.class).in(Scopes.SINGLETON);
                }
            }
            binder.bind(ConnectorServicesProvider.class).to(CoordinatorDynamicCatalogManager.class).in(Scopes.SINGLETON);
            binder.bind(CatalogManager.class).to(CoordinatorDynamicCatalogManager.class).in(Scopes.SINGLETON);
            binder.bind(CoordinatorLazyRegister.class).asEagerSingleton();

            configBinder(binder).bindConfig(CatalogPruneTaskConfig.class);
            binder.bind(CatalogPruneTask.class).in(Scopes.SINGLETON);
        }
        else {
            binder.bind(WorkerDynamicCatalogManager.class).in(Scopes.SINGLETON);
            binder.bind(ConnectorServicesProvider.class).to(WorkerDynamicCatalogManager.class).in(Scopes.SINGLETON);
            // catalog manager is not registered on worker
            binder.bind(WorkerLazyRegister.class).asEagerSingleton();
        }
    }

    private static class CoordinatorLazyRegister
    {
        @Inject
        public CoordinatorLazyRegister(
                DefaultCatalogFactory defaultCatalogFactory,
                LazyCatalogFactory lazyCatalogFactory,
                CoordinatorDynamicCatalogManager catalogManager,
                GlobalSystemConnector globalSystemConnector)
        {
            lazyCatalogFactory.setCatalogFactory(defaultCatalogFactory);
            catalogManager.registerGlobalSystemConnector(globalSystemConnector);
        }
    }

    private static class WorkerLazyRegister
    {
        @Inject
        public WorkerLazyRegister(
                DefaultCatalogFactory defaultCatalogFactory,
                LazyCatalogFactory lazyCatalogFactory,
                WorkerDynamicCatalogManager catalogManager,
                GlobalSystemConnector globalSystemConnector)
        {
            lazyCatalogFactory.setCatalogFactory(defaultCatalogFactory);
            catalogManager.registerGlobalSystemConnector(globalSystemConnector);
        }
    }
}
