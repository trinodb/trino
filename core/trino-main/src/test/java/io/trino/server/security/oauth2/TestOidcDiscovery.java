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
package io.trino.server.security.oauth2;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Key;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.trino.server.security.Authenticator;
import io.trino.server.security.oauth2.OAuth2ServerConfigProvider.OAuth2ServerConfig;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.server.ui.OAuth2WebUiAuthenticationFilter;
import io.trino.server.ui.WebUiAuthenticationFilter;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.http.client.HttpStatus.TOO_MANY_REQUESTS;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOidcDiscovery
{
    @Test
    public void testStaticConfiguration()
            throws Exception
    {
        testStaticConfiguration(Optional.empty(), Optional.empty(), Optional.empty());
        testStaticConfiguration(Optional.of("/access-token-issuer"), Optional.of("/userinfo"), Optional.empty());
        testStaticConfiguration(Optional.empty(), Optional.empty(), Optional.of("/connect/logout"));
        testStaticConfiguration(Optional.of("/access-token-issuer"), Optional.of("/userinfo"), Optional.of("/connect/logout"));
    }

    private void testStaticConfiguration(Optional<String> accessTokenPath, Optional<String> userinfoPath, Optional<String> endSessionPath)
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(ImmutableMap.of("/jwks.json", "jwk/jwk-public.json"))) {
            URI issuer = metadataServer.getBaseUrl();
            Optional<URI> accessTokenIssuer = accessTokenPath.map(issuer::resolve);
            Optional<URI> userinfoUrl = userinfoPath.map(issuer::resolve);
            Optional<URI> endSessionUrl = endSessionPath.map(issuer::resolve);
            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                    .put("http-server.authentication.oauth2.oidc.discovery", "false")
                    .put("http-server.authentication.oauth2.auth-url", issuer.resolve("/connect/authorize").toString())
                    .put("http-server.authentication.oauth2.token-url", issuer.resolve("/connect/token").toString())
                    .put("http-server.authentication.oauth2.jwks-url", issuer.resolve("/jwks.json").toString());
            accessTokenIssuer.map(URI::toString).ifPresent(uri -> properties.put("http-server.authentication.oauth2.access-token-issuer", uri));
            userinfoUrl.map(URI::toString).ifPresent(uri -> properties.put("http-server.authentication.oauth2.userinfo-url", uri));
            endSessionUrl.map(URI::toString).ifPresent(uri -> properties.put("http-server.authentication.oauth2.end-session-url", uri));
            try (TestingTrinoServer server = createServer(properties.buildOrThrow())) {
                assertConfiguration(server, issuer, accessTokenIssuer.map(issuer::resolve), userinfoUrl.map(issuer::resolve), endSessionUrl.map(issuer::resolve));
            }
        }
    }

    @Test
    public void testOidcDiscovery()
            throws Exception
    {
        testOidcDiscovery("openid-configuration.json",
                Optional.empty(),
                Optional.of("/connect/userinfo"),
                Optional.of("/connect/end_session"));
        testOidcDiscovery("openid-configuration-without-userinfo.json",
                Optional.empty(),
                Optional.empty(),
                Optional.of("/connect/end_session"));
        testOidcDiscovery("openid-configuration-with-access-token-issuer.json",
                Optional.of("http://access-token-issuer.com/adfs/services/trust"),
                Optional.of("/connect/userinfo"),
                Optional.of("/adfs/oauth2/logout"));
        testOidcDiscovery("openid-configuration-without-end-session-url.json",
                Optional.empty(),
                Optional.of("/connect/userinfo"),
                Optional.empty());
    }

    private void testOidcDiscovery(String configuration, Optional<String> accessTokenIssuer, Optional<String> userinfoUrl, Optional<String> endSessionUrl)
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(
                ImmutableMap.<String, String>builder()
                        .put("/.well-known/openid-configuration", "oidc/" + configuration)
                        .put("/jwks.json", "jwk/jwk-public.json")
                        .buildOrThrow());
                TestingTrinoServer server = createServer(
                        ImmutableMap.<String, String>builder()
                                .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                                .put("http-server.authentication.oauth2.oidc.discovery", "true")
                                .buildOrThrow())) {
            URI issuer = metadataServer.getBaseUrl();
            assertConfiguration(server, issuer, accessTokenIssuer.map(issuer::resolve), userinfoUrl.map(issuer::resolve), endSessionUrl.map(issuer::resolve));
        }
    }

    @Test
    public void testIssuerCheck()
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(
                ImmutableMap.<String, String>builder()
                        .put("/.well-known/openid-configuration", "oidc/openid-configuration-invalid-issuer.json")
                        .put("/jwks.json", "jwk/jwk-public.json")
                        .buildOrThrow());
                TestingTrinoServer server = createServer(
                        ImmutableMap.<String, String>builder()
                                .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                                .put("http-server.authentication.oauth2.oidc.discovery", "true")
                                .buildOrThrow())) {
            OAuth2ServerConfigProvider provider = server.getInstance(Key.get(OAuth2ServerConfigProvider.class));
            assertThatThrownBy(provider::get)
                    .hasMessageContaining(
                            "Invalid response from OpenID Metadata endpoint. " +
                                    "The value of the \"issuer\" claim in Metadata document different than the Issuer URL used for the Configuration Request.");
        }
    }

    @Test
    public void testStopOnClientError()
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(ImmutableMap.of());
                TestingTrinoServer server = createServer(
                        ImmutableMap.<String, String>builder()
                                .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                                .put("http-server.authentication.oauth2.oidc.discovery", "true")
                                .buildOrThrow())) {
            OAuth2ServerConfigProvider provider = server.getInstance(Key.get(OAuth2ServerConfigProvider.class));
            assertThatThrownBy(provider::get)
                    .hasMessageContaining("Invalid response from OpenID Metadata endpoint. Expected response code to be 200, but was 404");
        }
    }

    @Test
    public void testOidcDiscoveryRetrying()
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(new MetadataServletWithStartup(
                ImmutableMap.<String, String>builder()
                        .put("/.well-known/openid-configuration", "oidc/openid-configuration.json")
                        .put("/jwks.json", "jwk/jwk-public.json")
                        .buildOrThrow(), 5));
                TestingTrinoServer server = createServer(
                        ImmutableMap.<String, String>builder()
                                .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                                .put("http-server.authentication.oauth2.oidc.discovery", "true")
                                .put("http-server.authentication.oauth2.oidc.discovery.timeout", "10s")
                                .buildOrThrow())) {
            URI issuer = metadataServer.getBaseUrl();
            assertConfiguration(server, issuer, Optional.empty(), Optional.of(issuer.resolve("/connect/userinfo")), Optional.of(issuer.resolve("/connect/end_session")));
        }
    }

    @Test
    public void testOidcDiscoveryTimesOut()
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(new MetadataServletWithStartup(
                ImmutableMap.<String, String>builder()
                        .put("/.well-known/openid-configuration", "oidc/openid-configuration.json")
                        .put("/jwks.json", "jwk/jwk-public.json")
                        .buildOrThrow(), 60));
                TestingTrinoServer server = createServer(
                        ImmutableMap.<String, String>builder()
                                .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                                .put("http-server.authentication.oauth2.oidc.discovery", "true")
                                .put("http-server.authentication.oauth2.oidc.discovery.timeout", "5s")
                                .buildOrThrow())) {
            OAuth2ServerConfigProvider provider = server.getInstance(Key.get(OAuth2ServerConfigProvider.class));
            assertThatThrownBy(provider::get)
                    .hasMessageContaining("Invalid response from OpenID Metadata endpoint: 429");
        }
    }

    @Test
    public void testIgnoringUserinfoUrl()
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(
                ImmutableMap.<String, String>builder()
                        .put("/.well-known/openid-configuration", "oidc/openid-configuration.json")
                        .put("/jwks.json", "jwk/jwk-public.json")
                        .buildOrThrow());
                TestingTrinoServer server = createServer(
                        ImmutableMap.<String, String>builder()
                                .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                                .put("http-server.authentication.oauth2.oidc.discovery", "true")
                                .put("http-server.authentication.oauth2.oidc.use-userinfo-endpoint", "false")
                                .buildOrThrow())) {
            URI issuer = metadataServer.getBaseUrl();
            assertConfiguration(server, issuer, Optional.empty(), Optional.empty(), Optional.of(issuer.resolve("/connect/end_session")));
        }
    }

    @Test
    public void testBackwardCompatibility()
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(
                ImmutableMap.<String, String>builder()
                        .put("/.well-known/openid-configuration", "oidc/openid-configuration-with-access-token-issuer.json")
                        .put("/jwks.json", "jwk/jwk-public.json")
                        .buildOrThrow())) {
            URI issuer = metadataServer.getBaseUrl();
            URI authUrl = issuer.resolve("/custom-authorize");
            URI tokenUrl = issuer.resolve("/custom-token");
            URI jwksUrl = issuer.resolve("/custom-jwks.json");
            String accessTokenIssuer = issuer.resolve("/custom-access-token-issuer").toString();
            URI userinfoUrl = issuer.resolve("/custom-userinfo-url");
            try (TestingTrinoServer server = createServer(
                    ImmutableMap.<String, String>builder()
                            .put("http-server.authentication.oauth2.issuer", issuer.toString())
                            .put("http-server.authentication.oauth2.oidc.discovery", "true")
                            .put("http-server.authentication.oauth2.auth-url", authUrl.toString())
                            .put("http-server.authentication.oauth2.token-url", tokenUrl.toString())
                            .put("http-server.authentication.oauth2.jwks-url", jwksUrl.toString())
                            .put("http-server.authentication.oauth2.access-token-issuer", accessTokenIssuer)
                            .put("http-server.authentication.oauth2.userinfo-url", userinfoUrl.toString())
                            .buildOrThrow())) {
                assertComponents(server);
                OAuth2ServerConfig config = server.getInstance(Key.get(OAuth2ServerConfigProvider.class)).get();
                assertThat(config.accessTokenIssuer()).isEqualTo(Optional.of(accessTokenIssuer));
                assertThat(config.authUrl()).isEqualTo(authUrl);
                assertThat(config.tokenUrl()).isEqualTo(tokenUrl);
                assertThat(config.jwksUrl()).isEqualTo(jwksUrl);
                assertThat(config.userinfoUrl()).isEqualTo(Optional.of(userinfoUrl));
            }
        }
    }

    private static void assertConfiguration(TestingTrinoServer server, URI issuer, Optional<URI> accessTokenIssuer, Optional<URI> userinfoUrl, Optional<URI> endSessionUrl)
    {
        assertComponents(server);
        OAuth2ServerConfig config = server.getInstance(Key.get(OAuth2ServerConfigProvider.class)).get();
        assertThat(config.accessTokenIssuer()).isEqualTo(accessTokenIssuer.map(URI::toString));
        assertThat(config.authUrl()).isEqualTo(issuer.resolve("/connect/authorize"));
        assertThat(config.tokenUrl()).isEqualTo(issuer.resolve("/connect/token"));
        assertThat(config.jwksUrl()).isEqualTo(issuer.resolve("/jwks.json"));
        assertThat(config.userinfoUrl()).isEqualTo(userinfoUrl);
        assertThat(config.endSessionUrl()).isEqualTo(endSessionUrl);
    }

    private static void assertComponents(TestingTrinoServer server)
    {
        List<Authenticator> authenticators = server.getInstance(new Key<>() {});
        assertThat(authenticators).hasSize(1);
        assertThat(authenticators.get(0)).isInstanceOf(OAuth2Authenticator.class);
        assertThat(server.getInstance(Key.get(WebUiAuthenticationFilter.class))).isInstanceOf(OAuth2WebUiAuthenticationFilter.class);
        // does not throw an exception
        server.getInstance(Key.get(OAuth2Client.class)).load();
    }

    private static TestingTrinoServer createServer(Map<String, String> configuration)
    {
        return TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", Resources.getResource("cert/localhost.pem").getPath())
                        .put("http-server.https.keystore.key", "")
                        .put("http-server.process-forwarded", "true")
                        .put("http-server.authentication.allow-insecure-over-http", "true")
                        .put("http-server.authentication.type", "oauth2")
                        .put("http-server.authentication.oauth2.client-id", "another-consumer")
                        .put("http-server.authentication.oauth2.client-secret", "consumer-secret")
                        .putAll(configuration)
                        .buildOrThrow())
                .build();
    }

    public static class MetadataServer
            implements AutoCloseable
    {
        private final TestingHttpServer httpServer;

        public MetadataServer(Map<String, String> responseMapping)
                throws Exception
        {
            this(new MetadataServlet(responseMapping));
        }

        public MetadataServer(HttpServlet servlet)
                throws Exception
        {
            NodeInfo nodeInfo = new NodeInfo("test");
            HttpServerConfig config = new HttpServerConfig().setHttpPort(0);
            HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);
            httpServer = new TestingHttpServer(httpServerInfo, nodeInfo, config, servlet);
            httpServer.start();
        }

        public URI getBaseUrl()
        {
            return httpServer.getBaseUrl();
        }

        @Override
        public void close()
                throws Exception
        {
            httpServer.stop();
        }
    }

    public static class MetadataServlet
            extends HttpServlet
    {
        private final Map<String, String> responseMapping;

        public MetadataServlet(Map<String, String> responseMapping)
        {
            this.responseMapping = requireNonNull(responseMapping, "responseMapping is null");
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            String fileName = responseMapping.get(request.getPathInfo());
            if (fileName == null) {
                response.setStatus(404);
                return;
            }
            response.setHeader(CONTENT_TYPE, APPLICATION_JSON);
            String body = Resources.toString(Resources.getResource(fileName), UTF_8);
            body = body.replaceAll("https://issuer.com", request.getRequestURL().toString().replace("/.well-known/openid-configuration", ""));
            response.getWriter().write(body);
        }
    }

    public static class MetadataServletWithStartup
            extends MetadataServlet
    {
        private final Instant startTime;

        public MetadataServletWithStartup(Map<String, String> responseMapping, int startupInSeconds)
        {
            super(responseMapping);
            startTime = Instant.now().plusSeconds(startupInSeconds);
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            if (Instant.now().isBefore(startTime)) {
                response.setStatus(TOO_MANY_REQUESTS.code());
                return;
            }
            super.doGet(request, response);
        }
    }
}
