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
package io.trino.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.HydraIdentityProvider;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeOidcRefresh
        extends EnvironmentProvider
{
    private final PortBinder binder;
    private final HydraIdentityProvider hydraIdentityProvider;
    private final ResourceProvider configDir;

    @Inject
    public EnvSinglenodeOidcRefresh(DockerFiles dockerFiles, PortBinder binder, Standard standard, HydraIdentityProvider hydraIdentityProvider)
    {
        super(ImmutableList.of(standard, hydraIdentityProvider));
        this.binder = requireNonNull(binder, "binder is null");
        this.hydraIdentityProvider = requireNonNull(hydraIdentityProvider, "hydraIdentityProvider is null");
        requireNonNull(dockerFiles, "dockerFiles is null");
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/singlenode-oidc-refresh/");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(COORDINATOR, dockerContainer -> {
            dockerContainer
                    .withCopyFileToContainer(
                            forHostPath(configDir.getPath("config.properties")),
                            CONTAINER_TRINO_CONFIG_PROPERTIES);

            binder.exposePort(dockerContainer, 7778);
        });

        DockerContainer hydraClientConfig = hydraIdentityProvider.createClient(
                builder,
                "trinodb_client_id",
                "trinodb_client_secret",
                "client_secret_basic",
                "trinodb_client_id/",
                "https://presto-master:7778/oauth2/callback,https://localhost:7778/oauth2/callback");

        builder.containerDependsOn(COORDINATOR, hydraClientConfig.getLogicalName());
    }
}
