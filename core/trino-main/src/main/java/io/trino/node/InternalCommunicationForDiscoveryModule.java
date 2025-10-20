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
package io.trino.node;

import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.trino.server.InternalAuthenticationManager;
import io.trino.server.InternalCommunicationConfig;

import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.node.AddressToHostname.encodeAddressAsHostname;
import static io.trino.server.InternalCommunicationHttpClientModule.configureClient;

public class InternalCommunicationForDiscoveryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        InternalCommunicationConfig internalCommunicationConfig = buildConfigObject(InternalCommunicationConfig.class);
        Multibinder<HttpRequestFilter> discoveryFilterBinder = newSetBinder(binder, HttpRequestFilter.class, ForDiscoveryClient.class);
        if (internalCommunicationConfig.isHttpsRequired() && internalCommunicationConfig.getKeyStorePath() == null && internalCommunicationConfig.getTrustStorePath() == null) {
            discoveryFilterBinder.addBinding().to(DiscoveryEncodeAddressAsHostname.class);
        }
        configBinder(binder).bindConfigDefaults(HttpClientConfig.class, ForDiscoveryClient.class, config -> configureClient(config, internalCommunicationConfig));
        discoveryFilterBinder.addBinding().to(InternalAuthenticationManager.class);
    }

    private static class DiscoveryEncodeAddressAsHostname
            implements HttpRequestFilter
    {
        @Override
        public Request filterRequest(Request request)
        {
            return Request.Builder.fromRequest(request)
                    .setUri(toIpEncodedAsHostnameUri(request.getUri()))
                    .build();
        }

        private static URI toIpEncodedAsHostnameUri(URI uri)
        {
            if (!uri.getScheme().equals("https")) {
                return uri;
            }
            try {
                String host = uri.getHost();
                InetAddress inetAddress = InetAddress.getByName(host);
                String addressAsHostname = encodeAddressAsHostname(inetAddress);
                return new URI(uri.getScheme(), uri.getUserInfo(), addressAsHostname, uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
            }
            catch (UnknownHostException e) {
                throw new UncheckedIOException(e);
            }
            catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
