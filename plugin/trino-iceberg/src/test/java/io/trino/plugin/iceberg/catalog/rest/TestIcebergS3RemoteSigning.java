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
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequestParser;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponseParser;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogS3RemoteSignerProvider.EXTRA_CREDENTIALS_PREFIX;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergS3RemoteSigning
{
    private static final String KEY = "directory/object +.txt";
    private static final Location LOCATION = Location.of("s3://test-bucket/" + KEY);
    private static final byte[] CONTENT = "remotely signed content".getBytes(UTF_8);
    private static final String EXPIRED_TOKEN = "eyJhbGciOiJub25lIn0.eyJleHAiOjF9.c2lnbmF0dXJl";

    @Test
    void testReadUsesReturnedUriAndHeaders()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            TrinoInputFile inputFile = server.fileSystem("alice").newInputFile(LOCATION);
            assertThat(inputFile.length()).isEqualTo(CONTENT.length);
            try (TrinoInputStream input = inputFile.newStream()) {
                assertThat(input.readAllBytes()).isEqualTo(CONTENT);
            }

            assertThat(server.servlet.signingCalls)
                    .extracting(call -> call.request().method())
                    .containsExactly("HEAD", "GET");
            assertThat(server.servlet.signingCalls).allSatisfy(call -> {
                assertThat(call.authorization()).isEqualTo("Bearer alice");
                assertThat(call.request().provider()).isEqualTo("s3");
                assertThat(call.request().region()).isEqualTo("us-east-1");
                assertThat(call.request().uri().getPath()).isEqualTo("/unsigned/test-bucket/" + KEY);
                assertThat(call.request().headers().keySet()).noneMatch(header -> header.equalsIgnoreCase("Authorization"));
                assertThat(call.request().body()).isNull();
            });
            assertThat(server.servlet.storageCalls).hasSize(2).allSatisfy(call -> {
                assertThat(call.uri().getPath()).isEqualTo("/signed/test-bucket/" + KEY);
                assertThat(call.uri().getQuery()).contains("remote-signature=alice");
                assertThat(call.authorization()).isEqualTo("remote-signature-alice");
                assertThat(call.signedHeader()).isEqualTo("alice");
            });
        }
    }

    @Test
    void testSignaturesAreNotSharedBetweenIdentities()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            assertThat(read(server.fileSystem("alice"))).isEqualTo(CONTENT);
            assertThat(read(server.fileSystem("bob"))).isEqualTo(CONTENT);
            assertThatThrownBy(() -> read(server.fileSystem("denied")))
                    .isInstanceOf(ForbiddenException.class)
                    .hasMessage("Forbidden: Access denied");

            assertThat(server.servlet.signingCalls)
                    .extracting(SigningCall::authorization)
                    .containsExactly("Bearer alice", "Bearer bob", "Bearer denied");
            assertThat(server.servlet.signingCalls)
                    .extracting(call -> call.request().uri())
                    .containsOnly(server.servlet.signingCalls.getFirst().request().uri());
            assertThat(server.servlet.storageCalls)
                    .extracting(StorageCall::authorization)
                    .containsExactly("remote-signature-alice", "remote-signature-bob");
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
            assertThatThrownBy(() -> read(server.fileSystem(EXPIRED_TOKEN)))
                    .isInstanceOf(ForbiddenException.class)
                    .hasMessage("Forbidden: Access denied");

            assertThat(server.servlet.signingCalls)
                    .extracting(SigningCall::authorization)
                    .containsExactly("Bearer " + EXPIRED_TOKEN);
            assertThat(server.servlet.tokenRequests).isEmpty();
            assertThat(server.servlet.storageCalls).isEmpty();
        }
    }

    @Test
    void testTableTokenOverridesCatalogAuthorizationHeader()
            throws Exception
    {
        try (TestServer server = new TestServer(ImmutableMap.of(
                "token", "catalog-token",
                "header.authorization", "Bearer catalog-service"))) {
            assertThat(read(server.fileSystem("alice"))).isEqualTo(CONTENT);
            assertThatThrownBy(() -> read(server.fileSystem("denied")))
                    .isInstanceOf(ForbiddenException.class)
                    .hasMessage("Forbidden: Access denied");

            assertThat(server.servlet.signingCalls)
                    .extracting(SigningCall::authorization)
                    .containsExactly("Bearer alice", "Bearer denied");
            assertThat(server.servlet.storageCalls)
                    .extracting(StorageCall::authorization)
                    .containsExactly("remote-signature-alice");
        }
    }

    @Test
    void testConfiguredAuthorizationHeaderWithoutToken()
            throws Exception
    {
        try (TestServer server = new TestServer(ImmutableMap.of("header.authorization", "Bearer alice"))) {
            assertThat(read(server.fileSystem(ImmutableMap.of()))).isEqualTo(CONTENT);

            assertThat(server.servlet.signingCalls)
                    .extracting(SigningCall::authorization)
                    .containsExactly("Bearer alice");
            assertThat(server.servlet.storageCalls)
                    .extracting(StorageCall::authorization)
                    .containsExactly("remote-signature-alice");
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
            assertThat(read(server.fileSystem(ImmutableMap.of("token", EXPIRED_TOKEN, "scope", "table-scope"))))
                    .isEqualTo(CONTENT);

            assertThat(server.servlet.tokenRequests).singleElement().satisfies(body ->
                    assertThat(body)
                            .contains("grant_type=client_credentials", "scope=catalog-scope")
                            .doesNotContain("scope=sign", "scope=table-scope"));
            assertThat(server.servlet.signingCalls)
                    .extracting(SigningCall::authorization)
                    .containsExactly("Bearer alice");
        }
    }

    @Test
    void testWriteListAndDelete()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            TrinoFileSystem fileSystem = server.fileSystem("alice");
            fileSystem.newOutputFile(LOCATION).createOrOverwrite(CONTENT);
            FileIterator files = fileSystem.listFiles(Location.of("s3://test-bucket/directory/"));
            assertThat(files.hasNext()).isTrue();
            assertThat(files.next().location()).isEqualTo(LOCATION);
            assertThat(files.hasNext()).isFalse();
            fileSystem.deleteFile(LOCATION);

            assertThat(server.servlet.signingCalls)
                    .extracting(call -> call.request().method())
                    .containsExactly("PUT", "GET", "DELETE");
            assertThat(server.servlet.signingCalls.getFirst().request().body()).isNull();
            assertThat(server.servlet.storageCalls.getFirst().body()).isEqualTo(CONTENT);
            assertThat(server.servlet.storageCalls.get(1).uri().getQuery())
                    .contains("list-type=2", "prefix=directory/", "remote-signature=alice");
            assertThat(server.servlet.storageCalls).allSatisfy(call ->
                    assertThat(call.authorization()).isEqualTo("remote-signature-alice"));
        }
    }

    @Test
    void testBulkDeleteBodyIsSignedAndPreserved()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            server.fileSystem("alice").deleteFiles(ImmutableList.of(LOCATION, Location.of("s3://test-bucket/second-object")));

            assertThat(server.servlet.signingCalls).hasSize(1);
            RemoteSignRequest request = server.servlet.signingCalls.getFirst().request();
            assertThat(request.method()).isEqualTo("POST");
            assertThat(request.uri().getQuery()).matches("delete=?");
            assertThat(request.body()).contains("<Key>" + KEY + "</Key>", "<Key>second-object</Key>");
            assertThat(server.servlet.storageCalls).hasSize(1);
            assertThat(new String(server.servlet.storageCalls.getFirst().body(), UTF_8)).isEqualTo(request.body());
        }
    }

    @Test
    void testMultipartUpload()
            throws Exception
    {
        byte[] content = new byte[5 * 1024 * 1024 + 123];
        Arrays.fill(content, (byte) 'x');
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        try (TestServer server = new TestServer()) {
            try (OutputStream output = server.fileSystem("alice").newOutputFile(LOCATION).create(memoryContext)) {
                output.write(content);
            }

            assertThat(server.servlet.signingCalls)
                    .extracting(call -> call.request().method())
                    .containsExactly("POST", "PUT", "PUT", "POST");
            assertThat(server.servlet.signingCalls).allSatisfy(call -> assertThat(call.request().body()).isNull());
            assertThat(server.servlet.storageCalls).hasSize(4);
            assertThat(server.servlet.storageCalls.get(1).body()).isEqualTo(Arrays.copyOfRange(content, 0, 5 * 1024 * 1024));
            assertThat(server.servlet.storageCalls.get(2).body()).isEqualTo(Arrays.copyOfRange(content, 5 * 1024 * 1024, content.length));
            assertThat(new String(server.servlet.storageCalls.getLast().body(), UTF_8))
                    .contains("<CompleteMultipartUpload", "<PartNumber>1</PartNumber>", "<PartNumber>2</PartNumber>");
            assertThat(server.servlet.storageCalls).allSatisfy(call ->
                    assertThat(call.authorization()).isEqualTo("remote-signature-alice"));
        }
        finally {
            memoryContext.close();
        }
    }

    private static byte[] read(TrinoFileSystem fileSystem)
            throws IOException
    {
        try (TrinoInputStream input = fileSystem.newInputFile(LOCATION, CONTENT.length).newStream()) {
            return input.readAllBytes();
        }
    }

    private static final class TestServer
            implements AutoCloseable
    {
        private final SigningServlet servlet = new SigningServlet();
        private final TestingHttpServer server;
        private final IcebergRestCatalogS3RemoteSignerProvider signerProvider;
        private final S3FileSystemFactory fileSystemFactory;

        private TestServer()
                throws Exception
        {
            this(ImmutableMap.of("token", "catalog-token"));
        }

        private TestServer(Map<String, String> catalogProperties)
                throws Exception
        {
            NodeInfo nodeInfo = new NodeInfo("test");
            HttpServerConfig config = new HttpServerConfig().setHttpEnabled(true);
            HttpServerInfo serverInfo = new HttpServerInfo(config, Optional.of(new HttpConfig().setHttpPort(0)), Optional.empty(), nodeInfo);
            server = new TestingHttpServer("remote-signing", serverInfo, nodeInfo, config, servlet, ServerFeature.builder().build());
            server.start();

            signerProvider = new IcebergRestCatalogS3RemoteSignerProvider(ImmutableMap.<String, String>builder()
                    .put("uri", server.getBaseUrl().toString())
                    .put("rest.auth.type", "oauth2")
                    .put("token-refresh-enabled", "false")
                    .put("rest.client.max-retries", "1")
                    .putAll(catalogProperties)
                    .buildKeepingLast());
            fileSystemFactory = new S3FileSystemFactory(
                    OpenTelemetry.noop(),
                    new S3FileSystemConfig()
                            .setEndpoint(server.getBaseUrl().resolve("/unsigned").toString())
                            .setRegion("us-east-1")
                            .setPathStyleAccess(true)
                            .setMaxErrorRetries(1)
                            .setStreamingPartSize(DataSize.of(5, MEGABYTE)),
                    new S3FileSystemStats(),
                    Optional.of(signerProvider));
        }

        private TrinoFileSystem fileSystem(String token)
        {
            return fileSystem(ImmutableMap.of("token", token));
        }

        private TrinoFileSystem fileSystem(Map<String, String> signerProperties)
        {
            ImmutableMap.Builder<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                    .put(EXTRA_CREDENTIALS_PREFIX + "s3.remote-signing-enabled", "true");
            signerProperties.forEach((key, value) -> extraCredentials.put(EXTRA_CREDENTIALS_PREFIX + key, value));
            return fileSystemFactory.create(ConnectorIdentity.forUser("test")
                    .withExtraCredentials(extraCredentials.buildOrThrow())
                    .build());
        }

        @Override
        public void close()
                throws Exception
        {
            try (signerProvider) {
                fileSystemFactory.destroy();
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
        private final List<StorageCall> storageCalls = new CopyOnWriteArrayList<>();
        private final List<String> tokenRequests = new CopyOnWriteArrayList<>();

        @Override
        protected void service(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            if (request.getRequestURI().equals("/v1/oauth/tokens")) {
                tokenRequests.add(new String(request.getInputStream().readAllBytes(), UTF_8));
                response.setContentType("application/json");
                response.getWriter().write("{\"access_token\":\"alice\",\"token_type\":\"bearer\",\"expires_in\":3600}");
                return;
            }
            if (request.getRequestURI().equals("/v1/aws/s3/sign")) {
                sign(request, response);
                return;
            }

            byte[] body = request.getInputStream().readAllBytes();
            URI uri = URI.create(request.getRequestURI() + Optional.ofNullable(request.getQueryString()).map(query -> "?" + query).orElse(""));
            String authorization = request.getHeader("Authorization");
            storageCalls.add(new StorageCall(request.getMethod(), uri, authorization, request.getHeader("X-Remote-Signer"), body));
            String signedToken = request.getParameter("remote-signature");
            if (!request.getRequestURI().startsWith("/signed/") || signedToken == null ||
                    !signedToken.equals(request.getHeader("X-Remote-Signer")) ||
                    !("remote-signature-" + signedToken).equals(authorization)) {
                response.sendError(403);
                return;
            }

            response.setHeader("ETag", "\"part-etag\"");
            switch (request.getMethod()) {
                case "HEAD" -> {
                    response.setContentLength(CONTENT.length);
                    response.setHeader("Last-Modified", "Mon, 29 Sep 2025 10:00:00 GMT");
                }
                case "GET" -> {
                    if (request.getParameter("list-type") != null) {
                        response.setContentType("application/xml");
                        response.getWriter().write(
                                """
                                <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                  <Name>test-bucket</Name><IsTruncated>false</IsTruncated>
                                  <Contents><Key>%s</Key><Size>%s</Size><LastModified>2025-09-29T10:00:00Z</LastModified></Contents>
                                </ListBucketResult>
                                """.formatted(KEY, CONTENT.length));
                    }
                    else {
                        response.setContentLength(CONTENT.length);
                        response.getOutputStream().write(CONTENT);
                    }
                }
                case "POST" -> {
                    response.setContentType("application/xml");
                    if (request.getParameterMap().containsKey("delete")) {
                        response.getWriter().write("<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>");
                    }
                    else if (request.getParameterMap().containsKey("uploads")) {
                        response.getWriter().write("<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>");
                    }
                    else {
                        response.getWriter().write("<CompleteMultipartUploadResult><ETag>\"complete-etag\"</ETag></CompleteMultipartUploadResult>");
                    }
                }
                case "PUT", "DELETE" -> {}
                default -> response.sendError(405);
            }
        }

        private void sign(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            if (!request.getMethod().equals("POST")) {
                response.sendError(405);
                return;
            }
            RemoteSignRequest signRequest = RemoteSignRequestParser.fromJson(new String(request.getInputStream().readAllBytes(), UTF_8));
            String authorization = request.getHeader("Authorization");
            signingCalls.add(new SigningCall(authorization, signRequest));
            response.setContentType("application/json");
            response.setHeader("Cache-Control", "private");
            if (!ImmutableList.of("Bearer alice", "Bearer bob").contains(authorization)) {
                response.setStatus(403);
                response.getWriter().write("{\"error\":{\"message\":\"Access denied\",\"type\":\"ForbiddenException\",\"code\":403}}");
                return;
            }

            String token = authorization.substring("Bearer ".length());
            String signedUri = signRequest.uri().toString().replace("/unsigned/", "/signed/") +
                    (signRequest.uri().getRawQuery() == null ? "?" : "&") + "remote-signature=" + token;
            response.getWriter().write(RemoteSignResponseParser.toJson(ImmutableRemoteSignResponse.builder()
                    .uri(URI.create(signedUri))
                    .putHeaders("Authorization", ImmutableList.of("remote-signature-" + token))
                    .putHeaders("X-Remote-Signer", ImmutableList.of(token))
                    .build()));
        }
    }

    private record SigningCall(String authorization, RemoteSignRequest request) {}

    private record StorageCall(String method, URI uri, String authorization, String signedHeader, byte[] body) {}
}
