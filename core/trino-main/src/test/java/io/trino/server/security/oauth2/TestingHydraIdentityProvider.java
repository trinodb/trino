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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.scribejava.core.model.OAuthConstants;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.server.ui.WebUiModule;
import io.trino.util.AutoCloseableCloser;
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.MountableFile;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.client.OkHttpUtil.setupInsecureSsl;
import static io.trino.server.security.oauth2.TokenEndpointAuthMethod.CLIENT_SECRET_BASIC;
import static java.util.Objects.requireNonNull;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public class TestingHydraIdentityProvider
        implements AutoCloseable
{
    private static final String HYDRA_IMAGE = "oryd/hydra:v1.9.0-sqlite";
    private static final String ISSUER = "https://localhost:4444/";

    private final Network network = Network.newNetwork();
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final ObjectMapper mapper = new ObjectMapper();
    private final Duration ttlAccessToken;
    private final boolean useJwt;
    private final OkHttpClient httpClient;
    private GenericContainer<?> hydraContainer;

    public TestingHydraIdentityProvider()
    {
        this(Duration.ofMinutes(30), true);
    }

    public TestingHydraIdentityProvider(Duration ttlAccessToken, boolean useJwt)
    {
        this.ttlAccessToken = requireNonNull(ttlAccessToken, "ttlAccessToken is null");
        this.useJwt = useJwt;
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        setupInsecureSsl(httpClientBuilder);
        httpClientBuilder.followRedirects(false);
        httpClient = httpClientBuilder.build();
        closer.register(network);
    }

    public void start()
            throws Exception
    {
        TestingHttpServer loginAndConsentServer = createTestingLoginAndConsentServer();
        closer.register(loginAndConsentServer::stop);
        loginAndConsentServer.start();
        URI loginAndConsentBaseUrl = loginAndConsentServer.getBaseUrl();

        hydraContainer = createHydraContainer()
                .withNetworkAliases("hydra")
                .withExposedPorts(4444, 4445)
                .withEnv("DSN", "memory")
                .withEnv("URLS_SELF_ISSUER", ISSUER)
                .withEnv("URLS_CONSENT", loginAndConsentBaseUrl + "/consent")
                .withEnv("URLS_LOGIN", loginAndConsentBaseUrl + "/login")
                .withEnv("SERVE_TLS_KEY_PATH", "/tmp/certs/localhost.pem")
                .withEnv("SERVE_TLS_CERT_PATH", "/tmp/certs/localhost.pem")
                .withEnv("TTL_ACCESS_TOKEN", ttlAccessToken.getSeconds() + "s")
                .withEnv("STRATEGIES_ACCESS_TOKEN", useJwt ? "jwt" : null)
                .withCommand("serve", "all")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/cert"), "/tmp/certs")
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(".*Setting up http server on :4444.*", 1))
                        .withStrategy(Wait.forLogMessage(".*Setting up http server on :4445.*", 1)));
        closer.register(hydraContainer);
        hydraContainer.start();
    }

    public GenericContainer<?> createHydraContainer()
    {
        return new GenericContainer<>(HYDRA_IMAGE).withNetwork(network);
    }

    public void createClient(
            String clientId,
            String clientSecret,
            TokenEndpointAuthMethod tokenEndpointAuthMethod,
            List<String> audiences,
            String callbackUrl)
    {
        createHydraContainer()
                .withCommand("clients", "create",
                        "--endpoint", "https://hydra:4445",
                        "--skip-tls-verify",
                        "--id", clientId,
                        "--secret", clientSecret,
                        "--audience", String.join(",", audiences),
                        "--grant-types", "authorization_code,refresh_token,client_credentials",
                        "--response-types", "token,code,id_token",
                        "--scope", "openid,offline",
                        "--token-endpoint-auth-method", tokenEndpointAuthMethod.getValue(),
                        "--callbacks", callbackUrl)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(30)))
                .start();
    }

    public String getToken(String clientId, String clientSecret, List<String> audiences)
            throws IOException
    {
        try (Response response = httpClient
                .newCall(
                        new Request.Builder()
                                .url("https://localhost:" + getAuthPort() + "/oauth2/token")
                                .addHeader(OAuthConstants.HEADER, Credentials.basic(clientId, clientSecret))
                                .post(new FormBody.Builder()
                                        .add(OAuthConstants.GRANT_TYPE, OAuthConstants.CLIENT_CREDENTIALS)
                                        .add("audience", String.join(" ", audiences))
                                        .build())
                                .build())
                .execute()) {
            checkState(response.code() == SC_OK);
            requireNonNull(response.body());
            return mapper.readTree(response.body().byteStream())
                    .get("access_token")
                    .textValue();
        }
    }

    public int getAuthPort()
    {
        return hydraContainer.getMappedPort(4444);
    }

    public int getAdminPort()
    {
        return hydraContainer.getMappedPort(4445);
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }

    private TestingHttpServer createTestingLoginAndConsentServer()
            throws IOException
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig().setHttpPort(0);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);
        return new TestingHttpServer(httpServerInfo, nodeInfo, config, new AcceptAllLoginsAndConsentsServlet(), ImmutableMap.of());
    }

    private class AcceptAllLoginsAndConsentsServlet
            extends HttpServlet
    {
        private final ObjectMapper mapper = new ObjectMapper();
        private final OkHttpClient httpClient;

        public AcceptAllLoginsAndConsentsServlet()
        {
            OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
            setupInsecureSsl(httpClientBuilder);
            httpClient = httpClientBuilder.build();
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            if (request.getPathInfo().equals("/login")) {
                acceptLogin(request, response);
                return;
            }
            if (request.getPathInfo().contains("/consent")) {
                acceptConsent(request, response);
                return;
            }
            response.setStatus(SC_NOT_FOUND);
        }

        private void acceptLogin(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            String loginChallenge = request.getParameter("login_challenge");
            try (Response loginAcceptResponse = acceptLogin(loginChallenge)) {
                sendRedirect(loginAcceptResponse, response);
            }
        }

        private void acceptConsent(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            String consentChallenge = request.getParameter("consent_challenge");
            JsonNode consentRequest = getConsentRequest(consentChallenge);
            try (Response acceptConsentResponse = acceptConsent(consentChallenge, consentRequest)) {
                sendRedirect(acceptConsentResponse, response);
            }
        }

        private Response acceptLogin(String loginChallenge)
                throws IOException
        {
            return httpClient.newCall(
                            new Request.Builder()
                                    .url("https://localhost:" + getAdminPort() + "/oauth2/auth/requests/login/accept?login_challenge=" + loginChallenge)
                                    .put(RequestBody.create(
                                            MediaType.get(APPLICATION_JSON),
                                            mapper.writeValueAsString(mapper.createObjectNode().put("subject", "foo@bar.com"))))
                                    .build())
                    .execute();
        }

        private JsonNode getConsentRequest(String consentChallenge)
                throws IOException
        {
            try (Response response = httpClient.newCall(
                            new Request.Builder()
                                    .url("https://localhost:" + getAdminPort() + "/oauth2/auth/requests/consent?consent_challenge=" + consentChallenge)
                                    .get()
                                    .build())
                    .execute()) {
                requireNonNull(response.body());
                return mapper.readTree(response.body().byteStream());
            }
        }

        private Response acceptConsent(String consentChallenge, JsonNode consentRequest)
                throws IOException
        {
            return httpClient.newCall(
                            new Request.Builder()
                                    .url("https://localhost:" + getAdminPort() + "/oauth2/auth/requests/consent/accept?consent_challenge=" + consentChallenge)
                                    .put(RequestBody.create(
                                            MediaType.get(APPLICATION_JSON),
                                            mapper.writeValueAsString(mapper.createObjectNode()
                                                    .<ObjectNode>set("grant_scope", consentRequest.get("requested_scope"))
                                                    .<ObjectNode>set("grant_access_token_audience", consentRequest.get("requested_access_token_audience")))))
                                    .build())
                    .execute();
        }

        private void sendRedirect(Response redirectResponse, HttpServletResponse response)
                throws IOException
        {
            requireNonNull(redirectResponse.body());
            response.sendRedirect(
                    toHostUrl(mapper.readTree(redirectResponse.body().byteStream())
                            .get("redirect_to")
                            .textValue()));
        }

        private String toHostUrl(String url)
        {
            return HttpUrl.get(url)
                    .newBuilder()
                    .port(getAuthPort())
                    .toString();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        try (TestingHydraIdentityProvider service = new TestingHydraIdentityProvider()) {
            service.start();
            String authServerUrl = "https://localhost:" + service.getAuthPort();
            service.createClient(
                    "trino-client",
                    "trino-secret",
                    CLIENT_SECRET_BASIC,
                    ImmutableList.of("https://localhost:8443/ui"),
                    "https://localhost:8443/oauth2/callback");
            try (TestingTrinoServer ignored = TestingTrinoServer.builder()
                    .setCoordinator(true)
                    .setAdditionalModule(new WebUiModule())
                    .setProperties(
                            ImmutableMap.<String, String>builder()
                                    .put("web-ui.enabled", "true")
                                    .put("web-ui.authentication.type", "oauth2")
                                    .put("http-server.https.port", "8443")
                                    .put("http-server.https.enabled", "true")
                                    .put("http-server.https.keystore.path", Resources.getResource("cert/localhost.pem").getPath())
                                    .put("http-server.https.keystore.key", "")
                                    .put("http-server.authentication.type", "oauth2")
                                    .put("http-server.authentication.oauth2.issuer", ISSUER)
                                    .put("http-server.authentication.oauth2.auth-url", authServerUrl + "/oauth2/auth")
                                    .put("http-server.authentication.oauth2.token-url", authServerUrl + "/oauth2/token")
                                    .put("http-server.authentication.oauth2.jwks-url", authServerUrl + "/.well-known/jwks.json")
                                    .put("http-server.authentication.oauth2.client-id", "trino-client")
                                    .put("http-server.authentication.oauth2.client-secret", "trino-secret")
                                    .put("http-server.authentication.oauth2.user-mapping.pattern", "(.*)@.*")
                                    .put("oauth2-jwk.http-client.trust-store-path", Resources.getResource("cert/localhost.pem").getPath())
                                    .build())
                    .build()) {
                Thread.sleep(Long.MAX_VALUE);
            }
        }
    }
}
