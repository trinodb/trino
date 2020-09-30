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
package io.prestosql.tests.product.launcher.env.environment;

import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentConfig;
import io.prestosql.tests.product.launcher.env.EnvironmentProvider;
import io.prestosql.tests.product.launcher.env.common.EnvironmentExtender;
import io.prestosql.tests.product.launcher.testcontainers.PortBinder;
import io.prestosql.tests.product.launcher.testcontainers.SelectedPortWaitStrategy;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.time.Duration;
import java.util.List;

import static io.prestosql.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.prestosql.tests.product.launcher.env.EnvironmentContainers.LDAP;
import static io.prestosql.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_CONFIG_PROPERTIES;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public abstract class AbstractSinglenodeLdap
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final String imagesVersion;

    private static final int LDAP_PORT = 636;

    protected AbstractSinglenodeLdap(List<EnvironmentExtender> bases, DockerFiles dockerFiles, PortBinder portBinder, EnvironmentConfig environmentConfig)
    {
        super(bases);
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.imagesVersion = requireNonNull(environmentConfig, "environmentConfig is null").getImagesVersion();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String baseImage = format("prestodev/%s:%s", getBaseImage(), imagesVersion);

        builder.configureContainer(COORDINATOR, dockerContainer -> {
            dockerContainer.setDockerImageName(baseImage);

            dockerContainer.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath(getPasswordAuthenticatorConfigPath())),
                    CONTAINER_PRESTO_ETC + "/password-authenticator.properties");

            dockerContainer.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-ldap/config.properties")),
                    CONTAINER_PRESTO_CONFIG_PROPERTIES);

            portBinder.exposePort(dockerContainer, 8443);
        });

        builder.configureContainer(TESTS, dockerContainer -> {
            dockerContainer.setDockerImageName(baseImage);
            dockerContainer.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/tempto/tempto-configuration-for-docker-ldap.yaml")),
                    CONTAINER_TEMPTO_PROFILE_CONFIG);
        });

        DockerContainer container = new DockerContainer(baseImage, LDAP)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new SelectedPortWaitStrategy(LDAP_PORT))
                .withStartupTimeout(Duration.ofMinutes(5));
        portBinder.exposePort(container, LDAP_PORT);

        builder.addContainer(container);
    }

    protected String getBaseImage()
    {
        return "centos6-oj8-openldap";
    }

    protected DockerFiles getDockerFiles()
    {
        return dockerFiles;
    }

    protected String getImagesVersion()
    {
        return imagesVersion;
    }

    protected abstract String getPasswordAuthenticatorConfigPath();
}
