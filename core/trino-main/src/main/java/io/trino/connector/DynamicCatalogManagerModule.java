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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.multibindings.OptionalBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.CatalogManager;
import io.trino.server.ServerConfig;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

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
            OptionalBinder.newOptionalBinder(binder, Key.get(Duration.class, ForCatalogBuildDelay.class));
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
                GlobalSystemConnector globalSystemConnector,
                @ForCatalogBuildDelay Optional<Duration> buildDelay)
        {
            buildDelay.ifPresentOrElse(
                    delay -> lazyCatalogFactory.setCatalogFactory(new DelayingCatalogFactory(delay, defaultCatalogFactory)),
                    () -> lazyCatalogFactory.setCatalogFactory(defaultCatalogFactory));
            catalogManager.registerGlobalSystemConnector(globalSystemConnector);
        }
    }

    @VisibleForTesting
    @Retention(RUNTIME)
    @Target({ElementType.TYPE_USE, FIELD, PARAMETER, METHOD, TYPE_PARAMETER})
    @BindingAnnotation
    public @interface ForCatalogBuildDelay {}

    private static class DelayingCatalogFactory
            implements CatalogFactory
    {
        private final Duration delay;
        private final CatalogFactory delegate;

        public DelayingCatalogFactory(Duration delay, CatalogFactory delegate)
        {
            this.delay = delay;
            this.delegate = delegate;
        }

        @Override
        public void addConnectorFactory(ConnectorFactory connectorFactory)
        {
            delegate.addConnectorFactory(connectorFactory);
        }

        @Override
        public CatalogConnector createCatalog(CatalogProperties catalogProperties)
        {
            try {
                Thread.sleep(delay.toMillis());
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return delegate.createCatalog(catalogProperties);
        }

        @Override
        public CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector)
        {
            try {
                Thread.sleep(delay.toMillis());
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return delegate.createCatalog(catalogHandle, connectorName, connector);
        }
    }
}
