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
package io.trino.plugin.openlineage;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.trino.plugin.openlineage.transport.OpenLineageTransportConfig;
import io.trino.plugin.openlineage.transport.OpenLineageTransportCreator;
import io.trino.plugin.openlineage.transport.console.OpenLineageConsoleTransport;
import io.trino.plugin.openlineage.transport.http.OpenLineageHttpTransport;
import io.trino.plugin.openlineage.transport.http.OpenLineageHttpTransportConfig;
import io.trino.spi.eventlistener.EventListener;

import java.net.URI;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class OpenLineageListenerModule
        extends AbstractConfigurationAwareModule
{
    private static final URI OPEN_LINEAGE_PRODUCER = URI.create("https://github.com/trinodb/trino/plugin/trino-openlineage");

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(OpenLineageTransportConfig.class);
        binder.bind(OpenLineage.class).toInstance(new OpenLineage(OPEN_LINEAGE_PRODUCER));
        configBinder(binder).bindConfig(OpenLineageListenerConfig.class);
        binder.bind(OpenLineageClient.class).toProvider(OpenLineageClientProvider.class).in(Scopes.SINGLETON);
        binder.bind(EventListener.class)
                .to(OpenLineageListener.class)
                .in(Scopes.SINGLETON);

        install(switch (buildConfigObject(OpenLineageTransportConfig.class).getTransport()) {
            case CONSOLE -> new ConsoleTransportModule();
            case HTTP -> new HttpTransportModule();
        });
    }

    private static class ConsoleTransportModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(OpenLineageTransportCreator.class).to(OpenLineageConsoleTransport.class);
        }
    }

    private static class HttpTransportModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(OpenLineageHttpTransportConfig.class);
            binder.bind(OpenLineageTransportCreator.class).to(OpenLineageHttpTransport.class);
        }
    }
}
