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
import io.trino.plugin.openlineage.config.http.OpenLineageClientHttpTransportConfig;
import io.trino.plugin.openlineage.transport.OpenLineageTransport;

import java.net.URI;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OpenLineageHttpTransport
        implements OpenLineageTransport
{
    private final String url;
    private final String endpoint;
    private final int timeout;
    private final ApiKeyTokenProvider apiKey;
    private final Map<String, String> urlParams;
    private final Map<String, String> headers;

    private static class ApiKeyTokenProvider
            implements TokenProvider
    {
        private final String token;

        public ApiKeyTokenProvider(String token)
        {
            this.token = requireNonNull(token);
        }

        @Override
        public String getToken()
        {
            return format("Bearer %s", this.token);
        }
    }

    @Inject
    public OpenLineageHttpTransport(OpenLineageClientHttpTransportConfig config)
    {
        this.url = config.getUrl();
        this.endpoint = config.getEndpoint();
        this.timeout = (int) config.getTimeout().toMillis();
        this.apiKey = config.getApiKey().map(ApiKeyTokenProvider::new).orElse(null);
        this.urlParams = config.getUrlParams();
        this.headers = config.getHeaders();
    }

    @Override
    public HttpTransport buildTransport()
            throws Exception
    {
        return new HttpTransport(
                new HttpConfig(
                        new URI(this.url),
                        this.endpoint,
                        null,
                        this.timeout,
                        this.apiKey,
                        this.urlParams,
                        this.headers));
    }
}
