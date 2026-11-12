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
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthSession;
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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner.REGION_NAME;

final class IcebergS3RemoteSigner
        implements HttpSigner<AwsCredentialsIdentity>
{
    private static final String S3_PROVIDER = "s3";

    private final RESTClient httpClient;
    private final Supplier<AuthSession> authSession;
    private final String endpoint;
    private final Map<String, String> headers;
    private final Map<String, String> requestProperties;

    IcebergS3RemoteSigner(RESTClient httpClient, Supplier<AuthSession> authSession, IcebergRestCatalogS3RemoteSigningContext context)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.authSession = requireNonNull(authSession, "authSession is null");
        requireNonNull(context, "context is null");
        this.endpoint = context.endpoint().toString();
        this.requestProperties = context.properties();
        ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
        context.headers().forEach((name, values) -> headers.put(name, String.join(", ", values)));
        this.headers = headers.buildOrThrow();
    }

    @Override
    public SignedRequest sign(SignRequest<? extends AwsCredentialsIdentity> signRequest)
    {
        SdkHttpRequest request = signRequest.request();
        RemoteSignRequest signingRequest = ImmutableRemoteSignRequest.builder()
                .provider(S3_PROVIDER)
                .region(signRequest.requireProperty(REGION_NAME))
                .method(request.method().name())
                .uri(request.getUri())
                .headers(request.headers())
                .properties(requestProperties)
                .body(requestBody(signRequest))
                .build();
        RemoteSignResponse signed;
        try {
            signed = httpClient.withAuthSession(authSession.get()).post(
                    endpoint,
                    signingRequest,
                    RemoteSignResponse.class,
                    headers,
                    ErrorHandlers.defaultErrorHandler());
        }
        catch (RuntimeException e) {
            // Iceberg maps some signer responses to IllegalArgumentException or UnsupportedOperationException
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to remotely sign S3 request", e);
        }

        Map<String, List<String>> signedHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        signedHeaders.putAll(request.headers());
        signedHeaders.putAll(signed.headers());
        return SignedRequest.builder()
                .request(request.toBuilder()
                        .encodedPath("")
                        // Like Iceberg's signer client, keep the request query when the signed URI has none
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
