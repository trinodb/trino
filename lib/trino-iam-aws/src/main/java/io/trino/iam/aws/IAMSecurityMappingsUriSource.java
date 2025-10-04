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
package io.trino.iam.aws;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;

import java.net.URI;
import java.util.function.Supplier;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static java.util.Objects.requireNonNull;

public class IAMSecurityMappingsUriSource<T extends IAMSecurityMappings<E>, E extends IAMSecurityMapping, C extends IAMSecurityMappingConfig>
        implements Supplier<T>
{
    private final URI configUri;
    private final HttpClient httpClient;
    private final String jsonPointer;
    private final TypeReference<T> mappingsTypeReference;

    @Inject
    public IAMSecurityMappingsUriSource(C config, HttpClient httpClient, TypeReference<T> mappingsTypeReference)
    {
        this.configUri = config.getConfigUri().orElseThrow();
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.jsonPointer = config.getJsonPointer();
        this.mappingsTypeReference = mappingsTypeReference;
    }

    @Override
    public T get()
    {
        return parseJson(getRawJsonString(), jsonPointer, mappingsTypeReference);
    }

    @VisibleForTesting
    public String getRawJsonString()
    {
        Request request = prepareGet().setUri(configUri).build();
        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        int status = response.getStatusCode();
        if (status != HttpStatus.OK.code()) {
            throw new RuntimeException("Request to '%s' returned unexpected status code: %s".formatted(configUri, status));
        }
        return response.getBody();
    }
}
