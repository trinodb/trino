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
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.wait.strategy.Wait;

import static java.util.Objects.requireNonNull;

public class HydraIdentityProvider
        implements EnvironmentExtender
{
    private static final int TTL_ACCESS_TOKEN_IN_SECONDS = 60;
    private static final String HYDRA_IMAGE = "oryd/hydra:v1.9.0-sqlite";
    private final PortBinder binder;

    @Inject
    public HydraIdentityProvider(PortBinder binder)
    {
        this.binder = requireNonNull(binder, "binder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerContainer hydraConsent = new DockerContainer("oryd/hydra-login-consent-node:v1.4.2", "hydra-consent")
                .withEnv("HYDRA_ADMIN_URL", "http://hydra:4445")
                .withEnv("NODE_TLS_REJECT_UNAUTHORIZED", "0")
                .waitingFor(Wait.forHttp("/").forPort(3000).forStatusCode(200));

        binder.exposePort(hydraConsent, 3000);

        DockerContainer hydra = new DockerContainer(HYDRA_IMAGE, "hydra")
                .withEnv("LOG_LEAK_SENSITIVE_VALUES", "true")
                .withEnv("DSN", "memory")
                .withEnv("URLS_SELF_ISSUER", "http://hydra:4444/")
                .withEnv("URLS_CONSENT", "http://hydra-consent:3000/consent")
                .withEnv("URLS_LOGIN", "http://hydra-consent:3000/login")
                .withEnv("STRATEGIES_ACCESS_TOKEN", "jwt")
                .withEnv("TTL_ACCESS_TOKEN", TTL_ACCESS_TOKEN_IN_SECONDS + "s")
                .withCommand("serve all --dangerous-force-http")
                .waitingFor(Wait.forHttp("/health/ready").forPort(4444).forStatusCode(200));

        binder.exposePort(hydra, 4444);
        binder.exposePort(hydra, 4445);

        builder.addContainer(hydraConsent);
        builder.addContainer(hydra);

        builder.containerDependsOn(hydra.getLogicalName(), hydraConsent.getLogicalName());
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
                        "--endpoint", "http://hydra:4445",
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
