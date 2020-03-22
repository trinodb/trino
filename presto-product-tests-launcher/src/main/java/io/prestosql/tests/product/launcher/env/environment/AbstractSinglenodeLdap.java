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

import com.google.common.collect.ImmutableList;
import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.env.common.AbstractEnvironmentProvider;
import io.prestosql.tests.product.launcher.env.common.Hadoop;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.testcontainers.SelectedPortWaitStrategy;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.time.Duration;

import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_CONFIG_PROPERTIES;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static io.prestosql.tests.product.launcher.testcontainers.TestcontainersUtil.exposePort;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

public abstract class AbstractSinglenodeLdap
        extends AbstractEnvironmentProvider
{
    private final DockerFiles dockerFiles;
    private final String imagesVersion;

    private static final int LDAP_PORT = 636;

    protected AbstractSinglenodeLdap(DockerFiles dockerFiles, Standard standard, Hadoop hadoop, EnvironmentOptions environmentOptions)
    {
        super(ImmutableList.of(standard, hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.imagesVersion = requireNonNull(environmentOptions.imagesVersion, "environmentOptions.imagesVersion is null");
    }

    @Override
    protected void extendEnvironment(Environment.Builder builder)
    {
        String baseImage = format("prestodev/%s:%s", getBaseImage(), imagesVersion);

        builder.configureContainer("presto-master", dockerContainer -> {
            dockerContainer.setDockerImageName(baseImage);

            dockerContainer.withFileSystemBind(
                    dockerFiles.getDockerFilesHostPath(getPasswordAuthenticatorConfigPath()),
                    CONTAINER_PRESTO_ETC + "/password-authenticator.properties",
                    READ_ONLY);

            dockerContainer.withFileSystemBind(
                    dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-ldap/config.properties"),
                    CONTAINER_PRESTO_CONFIG_PROPERTIES,
                    READ_ONLY);

            exposePort(dockerContainer, 8443);
        });

        builder.configureContainer("tests", dockerContainer -> {
            dockerContainer.setDockerImageName(baseImage);
            dockerContainer.withFileSystemBind(
                    dockerFiles.getDockerFilesHostPath("conf/tempto/tempto-configuration-for-docker-ldap.yaml"),
                    CONTAINER_TEMPTO_PROFILE_CONFIG,
                    READ_ONLY);
        });

        DockerContainer container = new DockerContainer(baseImage)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new SelectedPortWaitStrategy(LDAP_PORT))
                .withStartupTimeout(Duration.ofMinutes(5));
        exposePort(container, LDAP_PORT);

        builder.addContainer("ldapserver", container);
    }

    protected String getBaseImage()
    {
        return "centos6-oj8-openldap";
    }

    protected abstract String getPasswordAuthenticatorConfigPath();
}
