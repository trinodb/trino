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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class OpenLineageHttpTransportConfig
{
    private URI url;
    private String endpoint;
    private Optional<String> apiKey = Optional.empty();
    private Duration timeout = new Duration(5000, TimeUnit.MILLISECONDS);
    private Map<String, String> headers = new HashMap<>();
    private Map<String, String> urlParams = new HashMap<>();

    @NotNull
    public URI getUrl()
    {
        return url;
    }

    @Config("openlineage-event-listener.transport.url")
    @ConfigDescription("URL of receiving server. Explicitly set the scheme https:// to use symmetric encryption")
    public OpenLineageHttpTransportConfig setUrl(URI url)
    {
        this.url = url;
        return this;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    @Config("openlineage-event-listener.transport.endpoint")
    @ConfigDescription("Custom path for API receiving the events.")
    public OpenLineageHttpTransportConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public Optional<String> getApiKey()
    {
        return apiKey;
    }

    @Config("openlineage-event-listener.transport.api-key")
    @ConfigDescription("API Key to use when authenticating against OpenLineage API")
    @ConfigSecuritySensitive
    public OpenLineageHttpTransportConfig setApiKey(String apiKey)
    {
        this.apiKey = Optional.ofNullable(apiKey);
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("1h")
    public Duration getTimeout()
    {
        return timeout;
    }

    @Config("openlineage-event-listener.transport.timeout")
    @ConfigDescription("Timeout when making HTTP Requests.")
    public OpenLineageHttpTransportConfig setTimeout(Duration timeout)
    {
        this.timeout = timeout;
        return this;
    }

    public Map<String, String> getHeaders()
    {
        return headers;
    }

    @Config("openlineage-event-listener.transport.headers")
    @ConfigDescription("List of custom custom HTTP headers provided as: \"Header-Name-1: header value 1, Header-Value-2: header value 2, ...\" ")
    public OpenLineageHttpTransportConfig setHeaders(List<String> headers)
    {
        try {
            this.headers = headers
                    .stream()
                    .collect(Collectors.toMap(keyValue -> keyValue.split(":", 2)[0], keyValue -> keyValue.split(":", 2)[1]));
        }
        catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(format("Cannot parse http headers from property openlineage-event-listener.transport.headers; value provided was %s, " +
                    "expected format is \"Header-Name-1: header value 1, Header-Value-2: header value 2, ...\"", String.join(", ", headers)), e);
        }
        return this;
    }

    public Map<String, String> getUrlParams()
    {
        return urlParams;
    }

    @Config("openlineage-event-listener.transport.url-params")
    @ConfigDescription("List of custom custom url params provided as: \"url-param-1: url param value 1, ...\" ")
    public OpenLineageHttpTransportConfig setUrlParams(List<String> urlParas)
    {
        try {
            this.urlParams = urlParas
                    .stream()
                    .collect(Collectors.toMap(kvs -> kvs.split(":", 2)[0], kvs -> kvs.split(":", 2)[1]));
        }
        catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(format("Cannot parse url params from property openlineage-event-listener.transport.url-params; value provided was %s, " +
                    "expected format is \"url-param-1: url param value 1, ...\"", String.join(", ", urlParas)), e);
        }
        return this;
    }
}
