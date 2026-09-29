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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.auth.signer.AwsSignerExecutionAttribute.SIGNING_REGION;

@SuppressWarnings("deprecation")
final class IcebergS3RemoteSigner
        implements Signer
{
    private final RESTClient httpClient;
    private final Supplier<AuthSession> authSession;
    private final String endpoint;
    private final Map<String, String> headers;

    IcebergS3RemoteSigner(RESTClient httpClient, Supplier<AuthSession> authSession, Map<String, String> properties)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.authSession = requireNonNull(authSession, "authSession is null");
        String baseUri = properties.getOrDefault("s3.signer.uri", properties.getOrDefault(RESTCatalogProperties.SIGNER_URI, properties.get(CatalogProperties.URI)));
        String endpointPath = properties.getOrDefault("s3.signer.endpoint", properties.getOrDefault(RESTCatalogProperties.SIGNER_ENDPOINT, "v1/aws/s3/sign"));
        this.endpoint = requireNonNull(RESTUtil.resolveEndpoint(baseUri, endpointPath), "signer endpoint is null");
        Map<String, String> signingHeaders = new HashMap<>(RESTUtil.configHeaders(properties));
        if (properties.containsKey(OAuth2Properties.TOKEN) || properties.containsKey(OAuth2Properties.CREDENTIAL)) {
            signingHeaders.keySet().removeIf("Authorization"::equalsIgnoreCase);
        }
        this.headers = ImmutableMap.copyOf(signingHeaders);
    }

    @Override
    public SdkHttpFullRequest sign(SdkHttpFullRequest request, ExecutionAttributes executionAttributes)
    {
        Region region = requireNonNull(executionAttributes.getAttribute(SIGNING_REGION), "signing region is null");
        RemoteSignRequest signingRequest = ImmutableRemoteSignRequest.builder()
                .provider("s3")
                .region(region.id())
                .method(request.method().name())
                .uri(request.getUri())
                .headers(request.headers())
                .properties(Map.of())
                .body(requestBody(request))
                .build();
        RemoteSignResponse signed = httpClient.withAuthSession(authSession.get()).post(
                endpoint,
                signingRequest,
                RemoteSignResponse.class,
                headers,
                ErrorHandlers.defaultErrorHandler());

        Map<String, List<String>> signedHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        signedHeaders.putAll(request.headers());
        signedHeaders.putAll(signed.headers());
        return request.toBuilder()
                .encodedPath("")
                .clearQueryParameters()
                .uri(signed.uri())
                .headers(signedHeaders)
                .build();
    }

    private static String requestBody(SdkHttpFullRequest request)
    {
        if (request.method() != SdkHttpMethod.POST || !request.rawQueryParameters().containsKey("delete")) {
            return null;
        }
        try (InputStream input = request.contentStreamProvider().orElseThrow().newStream()) {
            return new String(input.readAllBytes(), UTF_8);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to read bulk delete request for remote signing", e);
        }
    }
}
