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
package io.trino.hdfs.s3;

import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;

import java.net.URI;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class UriBasedS3SecurityMappingsProvider
        implements S3SecurityMappingsProvider
{
    private final URI configUri;
    private final HttpClient httpClient;
    private final S3SecurityMappingsParser parser;

    @Inject
    public UriBasedS3SecurityMappingsProvider(S3SecurityMappingConfig config, @ForS3SecurityMapping HttpClient httpClient)
    {
        this.configUri = config.getConfigFilePath().map(URI::create).orElseThrow(() -> new IllegalArgumentException("configUri not set"));
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.parser = new S3SecurityMappingsParser(config);
    }

    String getRawJsonString()
    {
        Request request = prepareGet().setUri(configUri).build();
        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        int status = response.getStatusCode();
        if (status != HttpStatus.OK.code()) {
            throw new IllegalStateException(format("Request to '%s' returned unexpected status code: '%d'", configUri, status));
        }
        return response.getBody();
    }

    @Override
    public S3SecurityMappings get()
    {
        return parser.parseJSONString(getRawJsonString());
    }
}
