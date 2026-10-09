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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.HttpConfig;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequestParser;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponseParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogS3RemoteSignerProvider.EXTRA_CREDENTIALS_PREFIX;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.list;
import static org.apache.iceberg.rest.RESTCatalogProperties.REMOTE_SIGNING_CONFIG;
import static org.apache.iceberg.rest.RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static software.amazon.awssdk.http.SdkHttpMethod.GET;
import static software.amazon.awssdk.http.SdkHttpMethod.PUT;
import static software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner.REGION_NAME;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

final class TestIcebergRestCatalogS3RemoteSignerProvider
{
    private static final String SIGNER_ENDPOINT = "v1/warehouse/namespaces/test/tables/table/sign";
    private static final URI OBJECT_URI = URI.create("https://storage.example/old/object%20%2B.txt?old=removed");
    private static final URI SIGNED_URI = URI.create("https://signed.example/new/object%20%2B.txt?signature=returned");
    private static final SignRequest<AwsCredentialsIdentity> REQUEST = SignRequest.<AwsCredentialsIdentity>builder(AnonymousCredentialsProvider.create().resolveCredentials())
            .request(SdkHttpRequest.builder().method(GET).uri(OBJECT_URI).build())
            .putProperty(REGION_NAME, US_EAST_1.id())
            .build();
    private static final String EXPIRED_TOKEN = "eyJhbGciOiJub25lIn0.eyJleHAiOjF9.c2lnbmF0dXJl";

    @Test
    void testReturnedUriAndHeaders()
            throws Exception
    {
        String body = "object contents";
        SignRequest<AwsCredentialsIdentity> request = REQUEST.toBuilder()
                .request(REQUEST.request().toBuilder()
                        .method(PUT)
                        .putHeader("X-Replaced", "original")
                        .putHeader("X-Unsigned", "preserved")
                        .build())
                .payload(ContentStreamProvider.fromUtf8String(body))
                .build();
        try (TestServer server = new TestServer()) {
            SignedRequest signed = server.sign(ImmutableMap.of("token", "alice"), request);

            assertThat(server.servlet.signingCalls).singleElement().satisfies(call -> {
                assertThat(call.request().provider()).isEqualTo("s3");
                assertThat(call.request().region()).isEqualTo("us-east-1");
                assertThat(call.request().method()).isEqualTo("PUT");
                assertThat(call.request().uri()).isEqualTo(OBJECT_URI);
                assertThat(call.request().headers()).containsEntry("X-Replaced", ImmutableList.of("original"));
                assertThat(call.request().properties()).isEmpty();
                assertThat(call.request().body()).isNull();
            });
            assertThat(signed.request().getUri()).isEqualTo(SIGNED_URI);
            assertThat(signed.request().rawQueryParameters()).doesNotContainKey("old");
            assertThat(signed.request().method()).isEqualTo(PUT);
            assertThat(signed.request().firstMatchingHeader("Authorization")).contains("signed-alice");
            assertThat(signed.request().firstMatchingHeader("X-Replaced")).contains("returned");
            assertThat(signed.request().headers().keySet()).filteredOn(name -> name.equalsIgnoreCase("X-Replaced")).hasSize(1);
            assertThat(signed.request().firstMatchingHeader("X-Unsigned")).contains("preserved");
            try (InputStream input = signed.payload().orElseThrow().newStream()) {
                assertThat(new String(input.readAllBytes(), UTF_8)).isEqualTo(body);
            }
        }
    }

    @Test
    void testSignaturesAreNotSharedBetweenTokens()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            assertThat(server.sign("alice").request().firstMatchingHeader("Authorization")).contains("signed-alice");
            assertThat(server.sign("bob").request().firstMatchingHeader("Authorization")).contains("signed-bob");
            assertThatThrownBy(() -> server.sign("denied"))
                    .isInstanceOf(ForbiddenException.class)
                    .hasMessage("Forbidden: Access denied");

            assertThat(server.servlet.signingCalls)
                    .extracting(SigningCall::authorization)
                    .containsExactly("Bearer alice", "Bearer bob", "Bearer denied");
            assertThat(server.servlet.signingCalls)
                    .extracting(call -> call.request().uri())
                    .containsOnly(OBJECT_URI);
        }
    }

    @Test
    void testSignaturesAreNotCached()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            HttpSigner<AwsCredentialsIdentity> signer = server.signerProvider.getSigner(identity(ImmutableMap.of(
                            "token", "alice",
                            REMOTE_SIGNING_ENDPOINT, SIGNER_ENDPOINT)))
                    .orElseThrow();
            SignRequest<AwsCredentialsIdentity> firstRange = REQUEST.toBuilder()
                    .request(REQUEST.request().toBuilder().putHeader("Range", "bytes=0-3").build())
                    .build();
            SignRequest<AwsCredentialsIdentity> secondRange = REQUEST.toBuilder()
                    .request(REQUEST.request().toBuilder().putHeader("Range", "bytes=4-7").build())
                    .build();

            assertThat(signer.sign(firstRange).request().firstMatchingHeader("Range")).contains("bytes=0-3");
            assertThat(signer.sign(firstRange).request().firstMatchingHeader("Range")).contains("bytes=0-3");
            assertThat(signer.sign(secondRange).request().firstMatchingHeader("Range")).contains("bytes=4-7");

            server.servlet.signingDenied = true;
            assertThatThrownBy(() -> signer.sign(firstRange))
                    .isInstanceOf(ForbiddenException.class)
                    .hasMessage("Forbidden: Access denied");

            assertThat(server.servlet.signingCalls)
                    .extracting(call -> call.request().headers().get("Range"))
                    .containsExactly(
                            ImmutableList.of("bytes=0-3"),
                            ImmutableList.of("bytes=0-3"),
                            ImmutableList.of("bytes=4-7"),
                            ImmutableList.of("bytes=0-3"));
            assertThat(server.servlet.signingCalls).allSatisfy(call -> {
                assertThat(call.authorization()).isEqualTo("Bearer alice");
                assertThat(call.request().method()).isEqualTo("GET");
                assertThat(call.request().uri()).isEqualTo(OBJECT_URI);
            });
        }
    }

    @Test
    void testCatalogTokenWithoutTableToken()
            throws Exception
    {
        try (TestServer server = new TestServer(ImmutableMap.of("token", "alice"))) {
            server.sign(ImmutableMap.of(), REQUEST);
            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer alice");
        }
    }

    @Test
    void testExpiredTableTokenCannotRefreshWithCatalogCredential()
            throws Exception
    {
        try (TestServer server = new TestServer(ImmutableMap.of(
                "token", "catalog-token",
                "credential", "catalog-client:catalog-secret",
                "token-refresh-enabled", "true",
                "token-exchange-enabled", "false"))) {
            assertThatThrownBy(() -> server.sign(EXPIRED_TOKEN))
                    .isInstanceOf(ForbiddenException.class)
                    .hasMessage("Forbidden: Access denied");

            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer " + EXPIRED_TOKEN);
            assertThat(server.servlet.tokenRequests).isEmpty();
        }
    }

    @Test
    void testTableTokenOverridesCatalogAuthorizationHeader()
            throws Exception
    {
        try (TestServer server = new TestServer(ImmutableMap.of(
                "token", "catalog-token",
                "header.authorization", "Bearer catalog-service"))) {
            assertThat(server.sign("alice").request().firstMatchingHeader("Authorization")).contains("signed-alice");
            assertThatThrownBy(() -> server.sign("denied"))
                    .isInstanceOf(ForbiddenException.class)
                    .hasMessage("Forbidden: Access denied");

            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer alice", "Bearer denied");
        }
    }

    @Test
    void testConfiguredAuthorizationHeaderWithoutToken()
            throws Exception
    {
        try (TestServer server = new TestServer(ImmutableMap.of("header.authorization", "Bearer alice"))) {
            assertThat(server.sign(ImmutableMap.of(), REQUEST).request().firstMatchingHeader("Authorization")).contains("signed-alice");
            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer alice");
        }
    }

    @Test
    void testRemoteSigningConfigPropertiesAndHeaders()
            throws Exception
    {
        String config = """
                {"properties":{"key":"value"},"headers":{"X-Signer-Context":["first","second"]}}
                """;
        try (TestServer server = new TestServer(ImmutableMap.of("header.x-signer-context", "legacy"))) {
            server.sign(ImmutableMap.of("token", "alice", REMOTE_SIGNING_CONFIG, config), REQUEST);

            assertThat(server.servlet.signingCalls).singleElement().satisfies(call -> {
                assertThat(call.request().properties()).containsExactlyEntriesOf(ImmutableMap.of("key", "value"));
                assertThat(call.signerContextHeaders()).containsExactly("first, second");
                assertThat(call.authorization()).isEqualTo("Bearer alice");
            });
        }
    }

    @Test
    void testRemoteSigningConfigAuthorizationOverridesTableToken()
            throws Exception
    {
        String config = """
                {"headers":{"authorization":["Bearer bob"]}}
                """;
        try (TestServer server = new TestServer(ImmutableMap.of("header.Authorization", "Bearer catalog-service"))) {
            SignedRequest signed = server.sign(ImmutableMap.of("token", "alice", REMOTE_SIGNING_CONFIG, config), REQUEST);

            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer bob");
            assertThat(signed.request().firstMatchingHeader("Authorization")).contains("signed-bob");
        }
    }

    @Test
    void testEmptyRemoteSigningConfig()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            server.sign(ImmutableMap.of("token", "alice", REMOTE_SIGNING_CONFIG, "{}"), REQUEST);

            assertThat(server.servlet.signingCalls).singleElement().satisfies(call -> {
                assertThat(call.request().properties()).isEmpty();
                assertThat(call.signerContextHeaders()).isEmpty();
                assertThat(call.authorization()).isEqualTo("Bearer alice");
            });
        }
    }

    @Test
    void testTokenRefreshUsesCatalogScope()
            throws Exception
    {
        try (TestServer server = new TestServer(ImmutableMap.of(
                "token", EXPIRED_TOKEN,
                "credential", "catalog-client:catalog-secret",
                "scope", "catalog-scope",
                "token-refresh-enabled", "true",
                "token-exchange-enabled", "false"))) {
            server.sign(ImmutableMap.of("token", EXPIRED_TOKEN, "scope", "table-scope"), REQUEST);

            assertThat(server.servlet.tokenRequests).singleElement().satisfies(request ->
                    assertThat(request.body())
                            .contains("grant_type=client_credentials", "scope=catalog-scope")
                            .doesNotContain("scope=sign", "scope=table-scope"));
            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer alice");
        }
    }

    @Test
    void testTokenRefreshDoesNotUseFirstTableAuthentication()
            throws Exception
    {
        try (TestServer server = new TestServer(ImmutableMap.of(
                "token", EXPIRED_TOKEN,
                "credential", "catalog-client:catalog-secret",
                "scope", "catalog-scope",
                "token-refresh-enabled", "true",
                "token-exchange-enabled", "false"))) {
            server.sign("alice");
            assertThat(server.servlet.tokenRequests).isEmpty();

            server.sign(ImmutableMap.of(), REQUEST);

            assertThat(server.servlet.tokenRequests).singleElement().satisfies(request -> {
                assertThat(request.authorization()).isNull();
                assertThat(request.body()).contains("grant_type=client_credentials", "client_id=catalog-client", "scope=catalog-scope");
            });
            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer alice", "Bearer alice");
        }
    }

    @ParameterizedTest
    @CsvSource({
            "POST, delete, <Delete><Object><Key>first</Key></Object><Object><Key>second</Key></Object></Delete>, true",
            "PUT, '', object contents, false",
            "POST, uploads, '', false",
            "PUT, partNumber=1&uploadId=upload, part contents, false",
            "POST, uploadId=upload, <CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>part</ETag></Part></CompleteMultipartUpload>, false",
    })
    void testRequestBody(SdkHttpMethod method, String query, String body, boolean includeBody)
            throws Exception
    {
        URI uri = URI.create("https://storage.example/object" + (query.isEmpty() ? "" : "?" + query));
        SignRequest<AwsCredentialsIdentity> request = REQUEST.toBuilder()
                .request(SdkHttpRequest.builder().method(method).uri(uri).build())
                .payload(ContentStreamProvider.fromUtf8String(body))
                .build();
        try (TestServer server = new TestServer()) {
            SignedRequest signed = server.sign(ImmutableMap.of("token", "alice"), request);

            assertThat(server.servlet.signingCalls).singleElement().satisfies(call ->
                    assertThat(call.request().body()).isEqualTo(includeBody ? body : null));
            try (InputStream input = signed.payload().orElseThrow().newStream()) {
                assertThat(new String(input.readAllBytes(), UTF_8)).isEqualTo(body);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSignerEndpoint(boolean absolute)
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            String endpoint = SIGNER_ENDPOINT;
            String expectedPath = "/catalog/" + SIGNER_ENDPOINT;
            if (absolute) {
                endpoint = server.server.getBaseUrl().resolve("/absolute/sign").toString();
                expectedPath = "/absolute/sign";
            }
            server.sign(ImmutableMap.of("token", "alice", REMOTE_SIGNING_ENDPOINT, endpoint), REQUEST);
            assertThat(server.servlet.signingCalls).extracting(SigningCall::path).containsExactly(expectedPath);
        }
    }

    @Test
    void testSignerRequiresEndpoint()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            assertThatThrownBy(() -> server.signerProvider.getSigner(identity(ImmutableMap.of("token", "alice"))))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("signer endpoint is null");
            assertThat(server.servlet.signingCalls).isEmpty();
        }
    }

    @Test
    void testSignerRequiresCatalogUri()
            throws IOException
    {
        try (HTTPClient client = HTTPClient.builder(ImmutableMap.of()).build()) {
            assertThatThrownBy(() -> new IcebergS3RemoteSigner(
                    client,
                    () -> AuthSession.EMPTY,
                    ImmutableMap.of(REMOTE_SIGNING_ENDPOINT, SIGNER_ENDPOINT)))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("catalog URI is null");
        }
    }

    private static ConnectorIdentity identity(Map<String, String> properties)
    {
        ImmutableMap.Builder<String, String> credentials = ImmutableMap.builder();
        properties.forEach((key, value) -> credentials.put(EXTRA_CREDENTIALS_PREFIX + key, value));
        return ConnectorIdentity.forUser("test").withExtraCredentials(credentials.buildOrThrow()).build();
    }

    private static final class TestServer
            implements AutoCloseable
    {
        private final SigningServlet servlet = new SigningServlet();
        private final TestingHttpServer server;
        private final IcebergRestCatalogS3RemoteSignerProvider signerProvider;

        public TestServer()
                throws Exception
        {
            this(ImmutableMap.of("token", "catalog-token"));
        }

        public TestServer(Map<String, String> properties)
                throws Exception
        {
            NodeInfo nodeInfo = new NodeInfo("test");
            HttpServerConfig config = new HttpServerConfig().setHttpEnabled(true);
            HttpServerInfo serverInfo = new HttpServerInfo(config, Optional.of(new HttpConfig().setHttpPort(0)), Optional.empty(), nodeInfo);
            server = new TestingHttpServer("remote-signing", serverInfo, nodeInfo, config, servlet, ServerFeature.builder().build());
            server.start();
            signerProvider = new IcebergRestCatalogS3RemoteSignerProvider(ImmutableMap.<String, String>builder()
                    .put("uri", server.getBaseUrl().resolve("/catalog").toString())
                    .put("rest.auth.type", "oauth2")
                    .put("token-refresh-enabled", "false")
                    .put("rest.client.max-retries", "1")
                    .putAll(properties)
                    .buildKeepingLast());
        }

        public SignedRequest sign(String token)
        {
            return sign(ImmutableMap.of("token", token), REQUEST);
        }

        public SignedRequest sign(Map<String, String> properties, SignRequest<AwsCredentialsIdentity> request)
        {
            Map<String, String> signerProperties = ImmutableMap.<String, String>builder()
                    .put(REMOTE_SIGNING_ENDPOINT, SIGNER_ENDPOINT)
                    .putAll(properties)
                    .buildKeepingLast();
            return signerProvider.getSigner(identity(signerProperties)).orElseThrow()
                    .sign(request);
        }

        @Override
        public void close()
                throws Exception
        {
            try {
                signerProvider.close();
            }
            finally {
                server.stop();
            }
        }
    }

    private static final class SigningServlet
            extends HttpServlet
    {
        private final List<SigningCall> signingCalls = new CopyOnWriteArrayList<>();
        private final List<TokenRequest> tokenRequests = new CopyOnWriteArrayList<>();
        private volatile boolean signingDenied;

        @Override
        protected void doPost(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            response.setContentType("application/json");
            if (request.getRequestURI().equals("/catalog/v1/oauth/tokens")) {
                tokenRequests.add(new TokenRequest(request.getHeader("Authorization"), new String(request.getInputStream().readAllBytes(), UTF_8)));
                response.getWriter().write("{\"access_token\":\"alice\",\"token_type\":\"bearer\",\"expires_in\":3600}");
                return;
            }

            RemoteSignRequest signingRequest = RemoteSignRequestParser.fromJson(new String(request.getInputStream().readAllBytes(), UTF_8));
            String authorization = request.getHeader("Authorization");
            signingCalls.add(new SigningCall(request.getRequestURI(), authorization, list(request.getHeaders("X-Signer-Context")), signingRequest));
            response.setHeader("Cache-Control", "private");
            if (signingDenied || !ImmutableList.of("Bearer alice", "Bearer bob").contains(authorization)) {
                response.setStatus(403);
                response.getWriter().write("{\"error\":{\"message\":\"Access denied\",\"type\":\"ForbiddenException\",\"code\":403}}");
                return;
            }
            response.getWriter().write(RemoteSignResponseParser.toJson(ImmutableRemoteSignResponse.builder()
                    .uri(SIGNED_URI)
                    .putHeaders("Authorization", ImmutableList.of("signed-" + authorization.substring("Bearer ".length())))
                    .putHeaders("x-replaced", ImmutableList.of("returned"))
                    .build()));
        }
    }

    private record SigningCall(String path, String authorization, List<String> signerContextHeaders, RemoteSignRequest request) {}

    private record TokenRequest(String authorization, String body) {}
}
