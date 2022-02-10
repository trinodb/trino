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
package io.trino.tests.product.launcher.env.common;

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import io.trino.tests.product.launcher.testcontainers.SelectedPortWaitStrategy;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isPrestoContainer;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class HydraIdentityProvider
        implements EnvironmentExtender
{
    private static final int TTL_ACCESS_TOKEN_IN_SECONDS = 5;
    private static final String HYDRA_IMAGE = "oryd/hydra:v1.10.6";
    private static final String DSN = "postgres://hydra:mysecretpassword@hydra-db:5432/hydra?sslmode=disable";
    private final PortBinder binder;
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public HydraIdentityProvider(PortBinder binder, DockerFiles dockerFiles)
    {
        this.binder = requireNonNull(binder, "binder is null");
        requireNonNull(dockerFiles, "dockerFiles is null");
        this.configDir = dockerFiles.getDockerFilesHostDirectory("common/hydra-identity-provider");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerContainer databaseContainer = new DockerContainer("postgres:14.0", "hydra-db")
                .withEnv("POSTGRES_USER", "hydra")
                .withEnv("POSTGRES_PASSWORD", "mysecretpassword")
                .withEnv("POSTGRES_DB", "hydra")
                .withExposedPorts(5432)
                .waitingFor(new SelectedPortWaitStrategy(5432));

        DockerContainer migrationContainer = new DockerContainer(HYDRA_IMAGE, "hydra-db-migration")
                .withCommand("migrate", "sql", "--yes", DSN)
                .dependsOn(databaseContainer)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy())
                .setTemporary(true);

        DockerContainer hydraConsent = new DockerContainer("python:3.10.1-alpine", "hydra-consent")
                .withCopyFileToContainer(forHostPath(configDir.getPath("login_and_consent_server.py")), "/")
                .withCommand("python", "/login_and_consent_server.py")
                .withExposedPorts(3000)
                .waitingFor(Wait.forHttp("/healthz").forPort(3000).forStatusCode(200));

        binder.exposePort(hydraConsent, 3000);

        DockerContainer hydra = new DockerContainer(HYDRA_IMAGE, "hydra")
                .withEnv("LOG_LEAK_SENSITIVE_VALUES", "true")
                .withEnv("DSN", DSN)
                .withEnv("URLS_SELF_ISSUER", "https://hydra:4444/")
                .withEnv("URLS_CONSENT", "http://hydra-consent:3000/consent")
                .withEnv("URLS_LOGIN", "http://hydra-consent:3000/login")
                .withEnv("SERVE_TLS_KEY_PATH", "/tmp/certs/hydra.pem")
                .withEnv("SERVE_TLS_CERT_PATH", "/tmp/certs/hydra.pem")
                .withEnv("STRATEGIES_ACCESS_TOKEN", "jwt")
                .withEnv("TTL_ACCESS_TOKEN", TTL_ACCESS_TOKEN_IN_SECONDS + "s")
                .withEnv("OAUTH2_ALLOWED_TOP_LEVEL_CLAIMS", "groups")
                .withCommand("serve", "all")
                .withCopyFileToContainer(forHostPath(configDir.getPath("cert/hydra.pem")), "/tmp/certs/hydra.pem")
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(".*Setting up http server on :4444.*", 1))
                        .withStrategy(Wait.forLogMessage(".*Setting up http server on :4445.*", 1)));

        binder.exposePort(hydra, 4444);
        binder.exposePort(hydra, 4445);

        builder.addContainers(databaseContainer, migrationContainer, hydraConsent, hydra);

        builder.containerDependsOn(hydra.getLogicalName(), hydraConsent.getLogicalName());
        builder.containerDependsOn(hydra.getLogicalName(), migrationContainer.getLogicalName());
        builder.containerDependsOn(hydra.getLogicalName(), databaseContainer.getLogicalName());

        builder.configureContainers(dockerContainer -> {
            if (isPrestoContainer(dockerContainer.getLogicalName())) {
                dockerContainer
                        .withCopyFileToContainer(
                                forHostPath(configDir.getPath("cert/trino.pem")),
                                CONTAINER_PRESTO_ETC + "/trino.pem")
                        .withCopyFileToContainer(
                                forHostPath(configDir.getPath("cert/hydra.pem")),
                                CONTAINER_PRESTO_ETC + "/hydra.pem");
            }
        });

        builder.configureContainer(TESTS, dockerContainer ->
                dockerContainer
                        .withCopyFileToContainer(
                                forHostPath(configDir.getPath("tempto-configuration-for-docker-oauth2.yaml")),
                                CONTAINER_TEMPTO_PROFILE_CONFIG)
                        .withCopyFileToContainer(
                                forHostPath(configDir.getPath("cert/truststore.jks")),
                                "/docker/presto-product-tests/truststore.jks"));
    }

    public DockerContainer createClient(
            Environment.Builder builder,
            String clientId,
            String clientSecret,
            String tokenEndpointAuthMethod,
            String audience,
            String callbackUrl)
    {
        DockerContainer clientCreatingContainer = new DockerContainer(HYDRA_IMAGE, "hydra-client-preparation")
                .withCommand("clients", "create",
                        "--endpoint", "https://hydra:4445",
                        "--skip-tls-verify",
                        "--id", clientId,
                        "--secret", clientSecret,
                        "--audience", audience,
                        "-g", "authorization_code,refresh_token,client_credentials",
                        "-r", "token,code,id_token",
                        "--scope", "openid,offline",
                        "--token-endpoint-auth-method", tokenEndpointAuthMethod,
                        "--callbacks", callbackUrl)
                .setTemporary(true);

        builder.addContainer(clientCreatingContainer);

        builder.containerDependsOn(clientCreatingContainer.getLogicalName(), "hydra");

        return clientCreatingContainer;
    }
}
