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
import org.apache.iceberg.rest.RemoteSigningConfig;
import org.apache.iceberg.rest.RemoteSigningConfigParser;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner.REGION_NAME;

final class IcebergS3RemoteSigner
        implements HttpSigner<AwsCredentialsIdentity>
{
    private final RESTClient httpClient;
    private final Supplier<AuthSession> authSession;
    private final String endpoint;
    private final Map<String, String> headers;
    private final Map<String, String> requestProperties;

    IcebergS3RemoteSigner(RESTClient httpClient, Supplier<AuthSession> authSession, Map<String, String> properties)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.authSession = requireNonNull(authSession, "authSession is null");
        this.endpoint = RESTUtil.resolveEndpoint(
                requireNonNull(properties.get(CatalogProperties.URI), "catalog URI is null"),
                requireNonNull(properties.get(RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT), "signer endpoint is null"));
        String remoteSigningConfigJson = properties.get(RESTCatalogProperties.REMOTE_SIGNING_CONFIG);
        RemoteSigningConfig remoteSigningConfig = remoteSigningConfigJson == null ? RemoteSigningConfig.EMPTY : RemoteSigningConfigParser.fromJson(remoteSigningConfigJson);
        this.requestProperties = remoteSigningConfig.properties();
        Map<String, String> signingHeaders = new HashMap<>(RESTUtil.configHeaders(properties));
        if (properties.containsKey(OAuth2Properties.TOKEN) || properties.containsKey(OAuth2Properties.CREDENTIAL)) {
            signingHeaders.keySet().removeIf("Authorization"::equalsIgnoreCase);
        }
        remoteSigningConfig.headers().forEach((name, values) -> {
            signingHeaders.keySet().removeIf(name::equalsIgnoreCase);
            signingHeaders.put(name, String.join(", ", values));
        });
        this.headers = ImmutableMap.copyOf(signingHeaders);
    }

    @Override
    public SignedRequest sign(SignRequest<? extends AwsCredentialsIdentity> signRequest)
    {
        SdkHttpRequest request = signRequest.request();
        RemoteSignRequest signingRequest = ImmutableRemoteSignRequest.builder()
                .provider("s3")
                .region(signRequest.requireProperty(REGION_NAME))
                .method(request.method().name())
                .uri(request.getUri())
                .headers(request.headers())
                .properties(requestProperties)
                .body(requestBody(signRequest))
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
        return SignedRequest.builder()
                .request(request.toBuilder()
                        .encodedPath("")
                        .clearQueryParameters()
                        .uri(signed.uri())
                        .headers(signedHeaders)
                        .build())
                .payload(signRequest.payload().orElse(null))
                .build();
    }

    @Override
    public CompletableFuture<AsyncSignedRequest> signAsync(AsyncSignRequest<? extends AwsCredentialsIdentity> request)
    {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Asynchronous remote signing is not supported"));
    }

    private static String requestBody(SignRequest<? extends AwsCredentialsIdentity> signRequest)
    {
        // Only bulk deletes need a body to authorize the object keys. Other requests identify
        // their targets in the URI, so object data is not sent to the signer.
        SdkHttpRequest request = signRequest.request();
        if (request.method() != SdkHttpMethod.POST || !request.rawQueryParameters().containsKey("delete")) {
            return null;
        }
        try (InputStream input = signRequest.payload().orElseThrow().newStream()) {
            return new String(input.readAllBytes(), UTF_8);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to read bulk delete request for remote signing", e);
        }
    }
}
