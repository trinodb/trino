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

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.openlineage.client.OpenLineageClient;
import io.trino.plugin.openlineage.transport.OpenLineageTransportCreator;

import static java.util.Objects.requireNonNull;

public class OpenLineageClientProvider
        implements Provider<OpenLineageClient>
{
    private final OpenLineageTransportCreator openLineageTransportCreator;
    private final OpenLineageListenerConfig listenerConfig;

    @Inject
    public OpenLineageClientProvider(
            OpenLineageTransportCreator openLineageTransportCreator,
            OpenLineageListenerConfig listenerConfig)
    {
        this.openLineageTransportCreator = requireNonNull(openLineageTransportCreator, "openLineageTransportCreator is null");
        this.listenerConfig = requireNonNull(listenerConfig, "listenerConfig is null");
    }

    @Override
    public OpenLineageClient get()
    {
        OpenLineageClient.Builder clientBuilder = OpenLineageClient.builder();
        clientBuilder.transport(openLineageTransportCreator.buildTransport());

        String[] disabledFacets = listenerConfig
                .getDisabledFacets()
                .stream()
                .map(OpenLineageTrinoFacet::asText)
                .toArray(String[]::new);

        clientBuilder.disableFacets(disabledFacets);

        return clientBuilder.build();
    }
}
