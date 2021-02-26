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
import io.trino.server.testing.TestingTrinoServer;
import io.trino.server.ui.WebUiModule;
import io.trino.util.AutoCloseableCloser;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;

import static io.trino.server.security.oauth2.TokenEndpointAuthMethod.CLIENT_SECRET_BASIC;

public class TestingHydraIdentityProvider
        implements AutoCloseable
{
    static final int TTL_ACCESS_TOKEN_IN_SECONDS = 5;
    private static final String HYDRA_IMAGE = "oryd/hydra:v1.9.0-sqlite";

    private final Network network = Network.newNetwork();

    private final FixedHostPortGenericContainer<?> consentContainer = new FixedHostPortGenericContainer<>("oryd/hydra-login-consent-node:v1.4.2")
            .withNetwork(network)
            .withNetworkAliases("consent")
            .withExposedPorts(3000)
            .withEnv("HYDRA_ADMIN_URL", "https://hydra:4445")
            .withEnv("NODE_TLS_REJECT_UNAUTHORIZED", "0")
            .waitingFor(Wait.forHttp("/").forStatusCode(200));

    private final FixedHostPortGenericContainer<?> hydraContainer = createHydraContainer()
            .withNetworkAliases("hydra")
            .withExposedPorts(4444, 4445)
            .withEnv("DSN", "memory")
            .withEnv("URLS_SELF_ISSUER", "https://hydra:4444/")
            .withEnv("URLS_CONSENT", "http://consent:3000/consent")
            .withEnv("URLS_LOGIN", "http://consent:3000/login")
            .withEnv("SERVE_TLS_KEY_PATH", "/tmp/certs/localhost.pem")
            .withEnv("SERVE_TLS_CERT_PATH", "/tmp/certs/localhost.pem")
            .withEnv("STRATEGIES_ACCESS_TOKEN", "jwt")
            .withEnv("TTL_ACCESS_TOKEN", TTL_ACCESS_TOKEN_IN_SECONDS + "s")
            .withCommand("serve", "all")
            .withCopyFileToContainer(MountableFile.forClasspathResource("/cert"), "/tmp/certs")
            .waitingFor(new WaitAllStrategy()
                    .withStrategy(Wait.forLogMessage(".*Setting up http server on :4444.*", 1))
                    .withStrategy(Wait.forLogMessage(".*Setting up http server on :4445.*", 1)));

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    TestingHydraIdentityProvider()
    {
        closer.register(network);
        closer.register(consentContainer);
        closer.register(hydraContainer);
    }

    public void start()
    {
        consentContainer.start();
        hydraContainer.start();
    }

    public FixedHostPortGenericContainer<?> createHydraContainer()
    {
        return new FixedHostPortGenericContainer<>(HYDRA_IMAGE).withNetwork(network);
    }

    public void createClient(
            String clientId,
            String clientSecret,
            TokenEndpointAuthMethod tokenEndpointAuthMethod,
            String audience,
            String callbackUrl)
    {
        createHydraContainer()
                .withCommand("clients", "create",
                        "--endpoint", "https://hydra:4445",
                        "--skip-tls-verify",
                        "--id", clientId,
                        "--secret", clientSecret,
                        "--audience", audience,
                        "-g", "authorization_code,refresh_token,client_credentials",
                        "-r", "token,code,id_token",
                        "--scope", "openid,offline",
                        "--token-endpoint-auth-method", tokenEndpointAuthMethod.getValue(),
                        "--callbacks", callbackUrl)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(30)))
                .start();
    }

    public String getToken(String clientId, String clientSecret, String audience)
    {
        FixedHostPortGenericContainer<?> container = createHydraContainer()
                .withCommand("token", "client",
                        "--endpoint", "https://hydra:4444",
                        "--skip-tls-verify",
                        "--client-id", clientId,
                        "--client-secret", clientSecret,
                        "--audience", audience)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(30)));
        container.start();
        return container.getLogs(OutputFrame.OutputType.STDOUT).replaceAll("\\s+", "");
    }

    public Network getNetwork()
    {
        return network;
    }

    public int getHydraPort()
    {
        return hydraContainer.getMappedPort(4444);
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }

    public static void main(String[] args)
            throws Exception
    {
        try (TestingHydraIdentityProvider service = new TestingHydraIdentityProvider()) {
            // expose containers ports & override environment variables
            service.consentContainer
                    .withFixedExposedPort(9020, 3000);
            service.hydraContainer
                    .withFixedExposedPort(9001, 4444)
                    .withFixedExposedPort(9002, 4445)
                    .withEnv("URLS_SELF_ISSUER", "https://localhost:9001/")
                    .withEnv("URLS_CONSENT", "http://localhost:9020/consent")
                    .withEnv("URLS_LOGIN", "http://localhost:9020/login")
                    .withEnv("TTL_ACCESS_TOKEN", "30m");
            service.start();
            service.createClient(
                    "trino-client",
                    "trino-secret",
                    CLIENT_SECRET_BASIC,
                    "https://localhost:8443/ui",
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
                                    .put("http-server.authentication.oauth2.auth-url", "https://localhost:9001/oauth2/auth")
                                    .put("http-server.authentication.oauth2.token-url", "https://localhost:9001/oauth2/token")
                                    .put("http-server.authentication.oauth2.jwks-url", "https://localhost:9001/.well-known/jwks.json")
                                    .put("http-server.authentication.oauth2.client-id", "trino-client")
                                    .put("http-server.authentication.oauth2.client-secret", "trino-secret")
                                    .put("http-server.authentication.oauth2.audience", "https://localhost:8443/ui")
                                    .put("http-server.authentication.oauth2.user-mapping.pattern", "(.*)@.*")
                                    .put("oauth2-jwk.http-client.trust-store-path", Resources.getResource("cert/localhost.pem").getPath())
                                    .build())
                    .build()) {
                Thread.sleep(Long.MAX_VALUE);
            }
        }
    }
}
