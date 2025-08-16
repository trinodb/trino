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
package io.trino.server.protocol.spooling;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.server.ServerConfig;
import io.trino.server.protocol.spooling.SpoolingConfig.SegmentRetrievalMode;
import io.trino.spi.spool.SpoolingManager;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.trino.server.protocol.spooling.QueryDataEncoder.EncoderSelector.noEncoder;
import static io.trino.server.protocol.spooling.SpoolingConfig.SegmentRetrievalMode.WORKER_PROXY;
import static java.util.Objects.requireNonNull;

public class SpoolingServerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new QueryDataEncodingModule());

        binder.bind(SpoolingManagerRegistry.class).in(Scopes.SINGLETON);
        OptionalBinder<SpoolingManager> spoolingManagerBinder = newOptionalBinder(binder, new TypeLiteral<>() {});
        newOptionalBinder(binder, SpoolingConfig.class);
        SpoolingEnabledConfig spoolingEnabledConfig = buildConfigObject(SpoolingEnabledConfig.class);
        if (!spoolingEnabledConfig.isEnabled()) {
            binder.bind(QueryDataEncoder.EncoderSelector.class).toInstance(noEncoder());
            return;
        }

        newSetBinder(binder, SystemSessionPropertiesProvider.class).addBinding().to(SpoolingSessionProperties.class).in(Scopes.SINGLETON);

        boolean isCoordinator = buildConfigObject(ServerConfig.class).isCoordinator();
        SpoolingConfig spoolingConfig = buildConfigObject(SpoolingConfig.class);
        binder.bind(QueryDataEncoder.EncoderSelector.class).to(PreferredQueryDataEncoderSelector.class).in(Scopes.SINGLETON);

        SegmentRetrievalMode mode = spoolingConfig.getRetrievalMode();
        if (isCoordinator) {
            jaxrsBinder(binder).bind(CoordinatorSegmentResource.class);
        }
        else if (mode == WORKER_PROXY) {
            jaxrsBinder(binder).bind(WorkerSegmentResource.class);
        }

        spoolingManagerBinder.setBinding().toProvider(SpoolingManagerProvider.class).in(Scopes.SINGLETON);
    }

    @ProvidesIntoSet
    @Singleton
    // Fully qualified so not to confuse with Guice's Module
    public static com.fasterxml.jackson.databind.Module queryDataJacksonModule()
    {
        return new ServerQueryDataJacksonModule();
    }

    private static class SpoolingManagerProvider
            implements Provider<SpoolingManager>
    {
        private final SpoolingManagerRegistry registry;
        private final SpoolingConfig config;

        @Inject
        public SpoolingManagerProvider(SpoolingManagerRegistry registry, SpoolingConfig config)
        {
            this.registry = requireNonNull(registry, "registry is null");
            this.config = requireNonNull(config, "config is null");
        }

        @Override
        public SpoolingManager get()
        {
            return new SpoolingManagerBridge(config, registry);
        }
    }
}
