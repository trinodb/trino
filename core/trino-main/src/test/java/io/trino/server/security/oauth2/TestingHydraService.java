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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;

import static java.time.Duration.ofMinutes;

public class TestingHydraService
        implements AutoCloseable
{
    static final int TTL_ACCESS_TOKEN_IN_SECONDS = 5;
    private static final String HYDRA_IMAGE = "oryd/hydra:v1.4.2";
    private static final String DSN = "postgres://hydra:mysecretpassword@database:5432/hydra?sslmode=disable";
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final Network network = Network.newNetwork();

    private final PostgreSQLContainer<?> databaseContainer = new PostgreSQLContainer<>()
            .withNetwork(network)
            .withNetworkAliases("database")
            .withUsername("hydra")
            .withPassword("mysecretpassword")
            .withDatabaseName("hydra");

    private final GenericContainer<?> migrationContainer = createHydraContainer()
            .withCommand("migrate sql --yes " + DSN)
            .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(ofMinutes(5)));

    private final FixedHostPortGenericContainer<?> consentContainer = new FixedHostPortGenericContainer<>("oryd/hydra-login-consent-node:v1.4.2")
            .withNetwork(network)
            .withNetworkAliases("consent")
            .withExposedPorts(3000)
            .withEnv("HYDRA_ADMIN_URL", "http://hydra:4445")
            .withEnv("NODE_TLS_REJECT_UNAUTHORIZED", "0")
            .waitingFor(Wait.forHttp("/").forStatusCode(200));

    private final FixedHostPortGenericContainer<?> hydraContainer = createHydraContainer()
            .withNetworkAliases("hydra")
            .withExposedPorts(4444, 4445)
            .withEnv("SECRETS_SYSTEM", generateSecret())
            .withEnv("DSN", DSN)
            .withEnv("URLS_SELF_ISSUER", "http://hydra:4444/")
            .withEnv("URLS_CONSENT", "http://consent:3000/consent")
            .withEnv("URLS_LOGIN", "http://consent:3000/login")
            .withEnv("OAUTH2_ACCESS_TOKEN_STRATEGY", "jwt")
            .withEnv("TTL_ACCESS_TOKEN", TTL_ACCESS_TOKEN_IN_SECONDS + "s")
            .withCommand("serve all --dangerous-force-http")
            .waitingFor(Wait.forHttp("/health/ready").forPort(4444).forStatusCode(200));

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    TestingHydraService()
    {
        closer.register(network);
        closer.register(databaseContainer);
        closer.register(migrationContainer);
        closer.register(consentContainer);
        closer.register(hydraContainer);
    }

    public void start()
    {
        databaseContainer.start();
        migrationContainer.start();
        consentContainer.start();
        hydraContainer.start();
    }

    public FixedHostPortGenericContainer<?> createHydraContainer()
    {
        return new FixedHostPortGenericContainer<>(HYDRA_IMAGE).withNetwork(network);
    }

    public void createConsumer(String callback)
    {
        createHydraContainer()
                .withCommand("clients create " +
                        "--endpoint http://hydra:4445 " +
                        "--id another-consumer " +
                        "--secret consumer-secret " +
                        "-g authorization_code,refresh_token " +
                        "-r token,code,id_token " +
                        "--scope openid,offline " +
                        "--callbacks " + callback)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(30)))
                .start();
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

    private static String generateSecret()
    {
        byte[] randomBytes = new byte[32];
        SECURE_RANDOM.nextBytes(randomBytes);
        return Base64.getEncoder().encodeToString(randomBytes);
    }

    public static void main(String[] args)
            throws Exception
    {
        try (TestingHydraService service = new TestingHydraService()) {
            // expose containers ports & override environment variables
            service.consentContainer
                    .withFixedExposedPort(9020, 3000);
            service.hydraContainer
                    .withFixedExposedPort(9001, 4444)
                    .withFixedExposedPort(9002, 4445)
                    .withEnv("URLS_SELF_ISSUER", "http://localhost:9001/")
                    .withEnv("URLS_CONSENT", "http://localhost:9020/consent")
                    .withEnv("URLS_LOGIN", "http://localhost:9020/login")
                    .withEnv("TTL_ACCESS_TOKEN", "30m");
            service.start();
            service.createConsumer("https://localhost:8443/oauth2/callback");
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
                                    .put("http-server.authentication.oauth2.auth-url", "http://localhost:9001/oauth2/auth")
                                    .put("http-server.authentication.oauth2.token-url", "http://localhost:9001/oauth2/token")
                                    .put("http-server.authentication.oauth2.jwks-url", "http://localhost:9001/.well-known/jwks.json")
                                    .put("http-server.authentication.oauth2.client-id", "another-consumer")
                                    .put("http-server.authentication.oauth2.client-secret", "consumer-secret")
                                    .put("http-server.authentication.oauth2.user-mapping.pattern", "(.*)@.*")
                                    .build())
                    .build()) {
                Thread.sleep(Long.MAX_VALUE);
            }
        }
    }
}
