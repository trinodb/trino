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
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeLdapAndFile
        extends AbstractEnvSinglenodeLdap
{
    @Inject
    public EnvSinglenodeLdapAndFile(Standard standard, Hadoop hadoop, DockerFiles dockerFiles, PortBinder portBinder, EnvironmentConfig config)
    {
        super(ImmutableList.of(standard, hadoop), dockerFiles, portBinder, config);
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        super.extendEnvironment(builder);
        ResourceProvider configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/singlenode-ldap-and-file");
        builder.configureContainer(COORDINATOR, dockerContainer -> {
            dockerContainer
                    .withCopyFileToContainer(
                            forHostPath(configDir.getPath("config.properties")),
                            CONTAINER_PRESTO_CONFIG_PROPERTIES)
                    .withCopyFileToContainer(
                            forHostPath(configDir.getPath("file-authenticator.properties")),
                            CONTAINER_PRESTO_ETC + "/file-authenticator.properties")
                    .withCopyFileToContainer(
                            forHostPath(configDir.getPath("password.db")),
                            CONTAINER_PRESTO_ETC + "/password.db");
        });

        configureTempto(builder, configDir);
    }

    @Override
    protected String getPasswordAuthenticatorConfigPath()
    {
        return "conf/environment/singlenode-ldap/password-authenticator.properties";
    }
}
