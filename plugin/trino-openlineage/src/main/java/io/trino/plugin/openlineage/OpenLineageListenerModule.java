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
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.openlineage.client.OpenLineageClient;
import io.trino.plugin.openlineage.transport.OpenLineageTransport;
import io.trino.plugin.openlineage.transport.console.OpenLineageConsoleTransport;
import io.trino.plugin.openlineage.transport.http.OpenLineageHttpTransport;
import io.trino.plugin.openlineage.transport.http.OpenLineageHttpTransportConfig;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.openlineage.OpenLineageTransport.CONSOLE;
import static io.trino.plugin.openlineage.OpenLineageTransport.HTTP;

public class OpenLineageListenerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(OpenLineageListener.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(OpenLineageListenerConfig.class);

        install(conditionalModule(
                OpenLineageListenerConfig.class,
                config -> config.getTransport().equals(CONSOLE),
                internalBinder -> internalBinder.bind(OpenLineageTransport.class).to(OpenLineageConsoleTransport.class)));

        install(conditionalModule(
                OpenLineageListenerConfig.class,
                config -> config.getTransport().equals(HTTP),
                internalBinder -> {
                    configBinder(internalBinder).bindConfig(OpenLineageHttpTransportConfig.class);
                    internalBinder.bind(OpenLineageTransport.class).to(OpenLineageHttpTransport.class);
                }));
    }

    @Provides
    @Singleton
    private OpenLineageClient getClient(OpenLineageListenerConfig listenerConfig, OpenLineageTransport openLineageTransport)
            throws Exception
    {
        OpenLineageClient.Builder clientBuilder = OpenLineageClient.builder();
        clientBuilder.transport(openLineageTransport.buildTransport());

        String[] disabledFacets = listenerConfig
                .getDisabledFacets()
                .stream()
                .map(OpenLineageTrinoFacet::asText)
                .toArray(String[]::new);

        clientBuilder.disableFacets(disabledFacets);

        return clientBuilder.build();
    }
}
