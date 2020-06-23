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
import io.prestosql.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.MountableFile;

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
import static io.prestosql.tests.product.launcher.docker.ContainerUtil.exposePort;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;
import static org.testcontainers.utility.MountableFile.forHostPath;

public final class Standard
        implements EnvironmentExtender
{
    public static final String CONTAINER_PRESTO_ETC = "/docker/presto-product-tests/conf/presto/etc";
    public static final String CONTAINER_PRESTO_JVM_CONFIG = CONTAINER_PRESTO_ETC + "/jvm.config";
    public static final String CONTAINER_PRESTO_ACCESS_CONTROL_PROPERTIES = CONTAINER_PRESTO_ETC + "/access-control.properties";
    public static final String CONTAINER_PRESTO_CONFIG_PROPERTIES = CONTAINER_PRESTO_ETC + "/config.properties";
    public static final String CONTAINER_TEMPTO_PROFILE_CONFIG = "/docker/presto-product-tests/conf/tempto/tempto-configuration-profile-config-file.yaml";

    private final PathResolver pathResolver;
    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;

    private final String imagesVersion;
    private final File serverPackage;
    private final boolean debug;

    @Inject
    public Standard(
            PathResolver pathResolver,
            DockerFiles dockerFiles,
            PortBinder portBinder,
            EnvironmentOptions environmentOptions)
    {
        this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        requireNonNull(environmentOptions, "environmentOptions is null");
        this.imagesVersion = requireNonNull(environmentOptions.imagesVersion, "environmentOptions.imagesVersion is null");
        this.serverPackage = requireNonNull(environmentOptions.serverPackage, "environmentOptions.serverPackage is null");
        checkArgument(serverPackage.getName().endsWith(".tar.gz"), "Currently only server .tar.gz package is supported");
        this.debug = environmentOptions.debug;
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
                        .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/standard/access-control.properties")), CONTAINER_PRESTO_ACCESS_CONTROL_PROPERTIES)
                        .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/standard/config.properties")), CONTAINER_PRESTO_CONFIG_PROPERTIES);

        portBinder.exposePort(container, 8080); // Presto default port

        if (debug) {
            enablePrestoJavaDebugger(container, 5005); // debug port
        }

        return container;
    }

    @SuppressWarnings("resource")
    private DockerContainer createTestsContainer()
    {
        DockerContainer container = new DockerContainer("prestodev/centos6-oj8:" + imagesVersion)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath()), "/docker/presto-product-tests")
                .withCommand("bash", "-xeuc", "echo 'No command provided' >&2; exit 69")
                .waitingFor(new WaitAllStrategy()) // don't wait
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy());

        return container;
    }

    @SuppressWarnings("resource")
    public static DockerContainer createPrestoContainer(DockerFiles dockerFiles, PathResolver pathResolver, File serverPackage, String dockerImageName)
    {
        return new DockerContainer(dockerImageName)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath()), "/docker/presto-product-tests")
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/presto/etc/jvm.config")), CONTAINER_PRESTO_JVM_CONFIG)
                // the server package is hundreds MB and file system bind is much more efficient
                .withFileSystemBind(pathResolver.resolvePlaceholders(serverPackage).getPath(), "/docker/presto-server.tar.gz", READ_ONLY)
                .withCommand("/docker/presto-product-tests/run-presto.sh")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofMinutes(5));
    }

    public static void enablePrestoJavaDebugger(DockerContainer container, int debugPort)
    {
        try {
            FileAttribute<Set<PosixFilePermission>> rwx = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx"));
            Path script = Files.createTempFile("enable-java-debugger", ".sh", rwx);
            Files.writeString(
                    script,
                    format(
                            "#!/bin/bash\n" +
                                    "printf '%%s\\n' '%s' >> '%s'\n",
                            "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:" + debugPort,
                            CONTAINER_PRESTO_JVM_CONFIG),
                    UTF_8);
            container.withCopyFileToContainer(MountableFile.forHostPath(script), "/docker/presto-init.d/enable-java-debugger.sh");

            // expose debug port unconditionally when debug is enabled
            exposePort(container, debugPort);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
