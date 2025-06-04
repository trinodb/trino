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

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.OpenLdap;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeLdapInsecure
        extends EnvironmentProvider
{
    protected final DockerFiles dockerFiles;
    private final PortBinder portBinder;

    @Inject
    public EnvSinglenodeLdapInsecure(Standard standard, Hadoop hadoop, OpenLdap openLdap, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standard, hadoop, openLdap);
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addPasswordAuthenticator("ldap", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-ldap-without-ssl/password-authenticator.properties")));
        builder.configureContainer(COORDINATOR, dockerContainer -> {
            dockerContainer.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-ldap/config.properties")),
                    CONTAINER_TRINO_CONFIG_PROPERTIES);

            portBinder.exposePort(dockerContainer, 8443);
        });

        builder.configureContainer(TESTS, dockerContainer -> {
            dockerContainer.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/tempto/tempto-configuration-for-docker-ldap.yaml")),
                    CONTAINER_TEMPTO_PROFILE_CONFIG);
        });
    }
}
