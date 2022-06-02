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

import io.airlift.log.Logger;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Debug;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.ServerPackage;
import io.trino.tests.product.launcher.env.SupportedTrinoJdk;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.CONTAINER_HEALTH_D;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.CONTAINER_TRINO_ACCESS_CONTROL_PROPERTIES;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.CONTAINER_TRINO_JVM_CONFIG;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.getWorkerNumber;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isCoordinator;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isWorker;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.worker;
import static io.trino.tests.product.launcher.testcontainers.PortBinder.unsafelyExposePort;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;
import static org.testcontainers.containers.wait.strategy.Wait.forHealthcheck;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class MultinodeProvider
{
    private static final Logger log = Logger.get(MultinodeProvider.class);

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final File serverPackage;
    private final SupportedTrinoJdk jdkVersion;
    private final String imagesVersion;
    private final boolean debug;

    @Inject
    public MultinodeProvider(
            DockerFiles dockerFiles,
            PortBinder portBinder,
            EnvironmentConfig environmentConfig,
            @ServerPackage File serverPackage,
            SupportedTrinoJdk jdkVersion,
            @Debug boolean debug)
    {
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.serverPackage = requireNonNull(serverPackage, "serverPackage is null");
        this.jdkVersion = requireNonNull(jdkVersion, "jdkVersion is null");
        this.imagesVersion = requireNonNull(environmentConfig, "environmentConfig is null").getImagesVersion();
        this.debug = debug;
    }

    /**
     * Single node environment does not do it well enough and some issues are
     * only exposed with multi node installations.
     *
     * By default we are creating single coordinator, single worker test clusters.
     */
    public EnvironmentExtender singleWorker()
    {
        return workers(1);
    }

    public EnvironmentExtender workers(int workersCount)
    {
        checkArgument(workersCount > 0, "WorkersCount should be greater than 0");

        String dockerImage = "ghcr.io/trinodb/testing/centos7-oj11:" + imagesVersion;

        return new EnvironmentExtender()
        {
            @Override
            public String getName()
            {
                return format("Multinode(%d)", workersCount);
            }

            @Override
            public void extendEnvironment(Environment.Builder builder)
            {
                builder.addContainers(createCoordinator(), createTestsContainer());

                // default catalogs copied from /docker/presto-product-tests
                builder.addConnector("blackhole");
                builder.addConnector("jmx");
                builder.addConnector("system");
                builder.addConnector("tpch");

                for (int workerNumber = 1; workerNumber <= workersCount; workerNumber++) {
                    builder.addContainers(createTrinoContainer(dockerFiles, dockerFiles.getDockerFilesHostDirectory("common/multinode/worker"), serverPackage, jdkVersion, debug, dockerImage, worker(workerNumber)));
                }
            }

            @SuppressWarnings("resource")
            private DockerContainer createCoordinator()
            {
                DockerContainer container =
                        createTrinoContainer(dockerFiles, dockerFiles.getDockerFilesHostDirectory("common/multinode/coordinator"), serverPackage, jdkVersion, debug, dockerImage, COORDINATOR)
                                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/multinode/access-control.properties")), CONTAINER_TRINO_ACCESS_CONTROL_PROPERTIES);

                portBinder.exposePort(container, 8080); // Trino default port
                return container;
            }

            @SuppressWarnings("resource")
            private DockerContainer createTestsContainer()
            {
                DockerContainer container = new DockerContainer(dockerImage, TESTS)
                        .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath()), "/docker/presto-product-tests")
                        .withCommand("bash", "-xeuc", "echo 'No command provided' >&2; exit 69")
                        .waitingFor(new WaitAllStrategy()) // don't wait
                        .withStartupCheckStrategy(new IsRunningStartupCheckStrategy());

                return container;
            }
        };
    }

    @SuppressWarnings("resource")
    public static DockerContainer createTrinoContainer(DockerFiles dockerFiles, DockerFiles.ResourceProvider configProvider, File serverPackage, SupportedTrinoJdk jdkVersion, boolean debug, String dockerImageName, String logicalName)
    {
        DockerContainer container = new DockerContainer(dockerImageName, logicalName)
                .withNetworkAliases(logicalName + ".docker.cluster")
                .withExposedLogPaths("/var/trino/var/log", "/var/log/container-health.log")
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath()), "/docker/presto-product-tests")
                .withCopyFileToContainer(forHostPath(configProvider.getPath("jvm.config")), CONTAINER_TRINO_JVM_CONFIG)
                .withCopyFileToContainer(forHostPath(configProvider.getPath("config.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("health-checks/trino-health-check.sh")), CONTAINER_HEALTH_D + "trino-health-check.sh")
                // the server package is hundreds MB and file system bind is much more efficient
                .withFileSystemBind(serverPackage.getPath(), "/docker/presto-server.tar.gz", READ_ONLY)
                .withEnv("JAVA_HOME", jdkVersion.getJavaHome())
                .withCommand("/docker/presto-product-tests/run-presto.sh")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingForAll(forLogMessage(".*======== SERVER STARTED ========.*", 1), forHealthcheck())
                .withStartupTimeout(Duration.ofMinutes(5));
        if (debug) {
            enableTrinoJavaDebugger(container);
        }
        else {
            container.withHealthCheck(dockerFiles.getDockerFilesHostPath("health-checks/health.sh"));
        }
        return container;
    }

    private static void enableTrinoJavaDebugger(DockerContainer dockerContainer)
    {
        String logicalName = dockerContainer.getLogicalName();

        int debugPort;
        if (isCoordinator(dockerContainer)) {
            debugPort = 5005;
        }
        else if (isWorker(dockerContainer)) {
            debugPort = 5008 + getWorkerNumber(dockerContainer);
        }
        else {
            throw new IllegalStateException("Cannot enable Java debugger for: " + logicalName);
        }

        enableTrinoJavaDebugger(dockerContainer, debugPort);
    }

    private static void enableTrinoJavaDebugger(DockerContainer container, int debugPort)
    {
        log.info("Enabling Java debugger for container: '%s' on port %d", container.getLogicalName(), debugPort);

        try {
            FileAttribute<Set<PosixFilePermission>> rwx = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx"));
            Path script = Files.createTempFile("enable-java-debugger", ".sh", rwx);
            script.toFile().deleteOnExit();
            Files.writeString(
                    script,
                    format(
                            "#!/bin/bash\n" +
                                    "printf '%%s\\n' '%s' >> '%s'\n",
                            "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:" + debugPort,
                            CONTAINER_TRINO_JVM_CONFIG),
                    UTF_8);
            container.withCopyFileToContainer(forHostPath(script), "/docker/presto-init.d/enable-java-debugger.sh");

            // expose debug port unconditionally when debug is enabled
            unsafelyExposePort(container, debugPort);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
