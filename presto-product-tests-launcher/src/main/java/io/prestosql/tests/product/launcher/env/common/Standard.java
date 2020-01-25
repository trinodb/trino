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
package io.prestosql.tests.product.launcher.env.common;

import io.prestosql.tests.product.launcher.PathResolver;
import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

import javax.inject.Inject;

import java.io.File;
import java.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.tests.product.launcher.testcontainers.TestcontainersUtil.exposePort;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

public final class Standard
        implements EnvironmentExtender
{
    public static final String CONTAINER_PRESTO_ETC = "/docker/presto-product-tests/conf/presto/etc";
    public static final String CONTAINER_PRESTO_JVM_CONFIG = CONTAINER_PRESTO_ETC + "/jvm.config";
    public static final String CONTAINER_PRESTO_CONFIG_PROPERTIES = CONTAINER_PRESTO_ETC + "/config.properties";
    public static final String CONTAINER_TEMPTO_PROFILE_CONFIG = "/docker/presto-product-tests/conf/tempto/tempto-configuration-profile-config-file.yaml";

    private final PathResolver pathResolver;
    private final DockerFiles dockerFiles;

    private final String imagesVersion;
    private final File serverPackage;

    @Inject
    public Standard(
            PathResolver pathResolver,
            DockerFiles dockerFiles,
            EnvironmentOptions environmentOptions)
    {
        this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        requireNonNull(environmentOptions, "environmentOptions is null");
        imagesVersion = requireNonNull(environmentOptions.imagesVersion, "environmentOptions.imagesVersion is null");
        serverPackage = requireNonNull(environmentOptions.serverPackage, "environmentOptions.serverPackage is null");
        checkArgument(serverPackage.getName().endsWith(".tar.gz"), "Currently only server .tar.gz package is supported");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer("presto-master", createPrestoMaster());
        builder.addContainer("tests", createTestsContainer());
    }

    @SuppressWarnings("resource")
    private DockerContainer createPrestoMaster()
    {
        DockerContainer container =
                createPrestoContainer(dockerFiles, pathResolver, serverPackage, "prestodev/centos7-oj11:" + imagesVersion)
                .withFileSystemBind(dockerFiles.getDockerFilesHostPath("common/standard/config.properties"), CONTAINER_PRESTO_CONFIG_PROPERTIES, READ_ONLY);

        exposePort(container, 8080); // Presto default port
        exposePort(container, 5005); // debug port

        return container;
    }

    @SuppressWarnings("resource")
    private DockerContainer createTestsContainer()
    {
        DockerContainer container = new DockerContainer("prestodev/centos6-oj8:" + imagesVersion)
                .withFileSystemBind(dockerFiles.getDockerFilesHostPath(), "/docker/presto-product-tests", READ_ONLY)
                .withCommand("bash", "-xeuc", "echo 'No command provided' >&2; exit 69")
                .waitingFor(new WaitAllStrategy()) // don't wait
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy());

        exposePort(container, 5007); // debug port

        return container;
    }

    @SuppressWarnings("resource")
    public static DockerContainer createPrestoContainer(DockerFiles dockerFiles, PathResolver pathResolver, File serverPackage, String dockerImageName)
    {
        return new DockerContainer(dockerImageName)
                .withFileSystemBind(dockerFiles.getDockerFilesHostPath(), "/docker/presto-product-tests", READ_ONLY)
                .withFileSystemBind(pathResolver.resolvePlaceholders(serverPackage).toString(), "/docker/presto-server.tar.gz", READ_ONLY)
                .withCommand("/docker/presto-product-tests/run-presto.sh")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofMinutes(5));
    }
}
