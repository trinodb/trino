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
package io.trino.plugin.openlineage.transport.http;

import com.google.inject.Inject;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.TokenProvider;
import io.trino.plugin.openlineage.transport.OpenLineageTransport;

import java.net.URI;
import java.util.Map;

import static java.lang.Math.toIntExact;

public class OpenLineageHttpTransport
        implements OpenLineageTransport
{
    private final URI url;
    private final String endpoint;
    private final int timeout;
    private final TokenProvider tokenProvider;
    private final Map<String, String> urlParams;
    private final Map<String, String> headers;

    @Inject
    public OpenLineageHttpTransport(OpenLineageHttpTransportConfig config)
    {
        this.url = config.getUrl();
        this.endpoint = config.getEndpoint();
        this.timeout = toIntExact(config.getTimeout().toMillis());
        this.tokenProvider = config.getApiKey().map(OpenLineageHttpTransport::createTokenProvider).orElse(null);
        this.urlParams = config.getUrlParams();
        this.headers = config.getHeaders();
    }

    @Override
    public HttpTransport buildTransport()
    {
        return new HttpTransport(
                new HttpConfig(
                        this.url,
                        this.endpoint,
                        null,
                        this.timeout,
                        this.tokenProvider,
                        this.urlParams,
                        this.headers,
                        null));
    }

    private static TokenProvider createTokenProvider(String token)
    {
        return () -> "Bearer " + token;
    }
}
