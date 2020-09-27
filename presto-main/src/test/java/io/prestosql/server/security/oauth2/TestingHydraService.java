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
package io.prestosql.server.security.oauth2;

import io.prestosql.util.AutoCloseableCloser;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.Closeable;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Base64;

import static java.time.Duration.ofMinutes;

public class TestingHydraService
        implements Closeable
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

    private final GenericContainer<?> consentContainer = new GenericContainer<>("oryd/hydra-login-consent-node:v1.4.2")
            .withNetwork(network)
            .withNetworkAliases("consent")
            .withExposedPorts(3000)
            .withEnv("HYDRA_ADMIN_URL", "http://hydra:4445")
            .withEnv("NODE_TLS_REJECT_UNAUTHORIZED", "0")
            .waitingFor(Wait.forHttp("/").forStatusCode(200));

    private final GenericContainer<?> hydraContainer = createHydraContainer()
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

    public GenericContainer<?> createHydraContainer()
    {
        return new GenericContainer<>(HYDRA_IMAGE).withNetwork(network);
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
            throws IOException
    {
        try {
            closer.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String generateSecret()
    {
        byte[] randomBytes = new byte[32];
        SECURE_RANDOM.nextBytes(randomBytes);
        return Base64.getEncoder().encodeToString(randomBytes);
    }
}
