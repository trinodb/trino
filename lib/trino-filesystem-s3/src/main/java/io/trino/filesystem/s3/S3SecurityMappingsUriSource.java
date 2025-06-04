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
package io.trino.filesystem.s3;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.trino.filesystem.s3.S3FileSystemModule.ForS3SecurityMapping;

import java.net.URI;
import java.util.function.Supplier;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static java.util.Objects.requireNonNull;

class S3SecurityMappingsUriSource
        implements Supplier<S3SecurityMappings>
{
    private final URI configUri;
    private final HttpClient httpClient;
    private final String jsonPointer;

    @Inject
    public S3SecurityMappingsUriSource(S3SecurityMappingConfig config, @ForS3SecurityMapping HttpClient httpClient)
    {
        this.configUri = config.getConfigUri().orElseThrow();
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.jsonPointer = config.getJsonPointer();
    }

    @Override
    public S3SecurityMappings get()
    {
        return parseJson(getRawJsonString(), jsonPointer, S3SecurityMappings.class);
    }

    @VisibleForTesting
    String getRawJsonString()
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
