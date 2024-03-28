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
package io.trino.server.rpm;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.testing.containers.TestContainers.DockerArchitecture;
import org.assertj.core.api.AbstractAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static io.trino.server.rpm.ServerIT.PathInfoAssert.assertThatPaths;
import static io.trino.testing.TestingProperties.getProjectVersion;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.containers.TestContainers.getDockerArchitectureInfo;
import static java.lang.String.format;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.sql.DriverManager.getConnection;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;

@Execution(SAME_THREAD)
public class ServerIT
{
    private static final DockerImageName BASE_IMAGE = DockerImageName.parse("registry.access.redhat.com/ubi9/ubi-minimal:latest");
    private static final DockerArchitecture ARCH = getDockerArchitectureInfo(BASE_IMAGE).imageArch();

    private final String rpmHostPath;

    public ServerIT()
    {
        rpmHostPath = requireNonNull(System.getProperty("rpm"), "rpm is null");
    }

    @Test
    public void testInstallUninstall()
            throws Exception
    {
        // Release names as in the https://api.adoptium.net/q/swagger-ui/#/Release%20Info/getReleaseNames
        testInstall("jdk-22+36", "/usr/lib/jvm/temurin-22", "22");
        testUninstall("jdk-22+36", "/usr/lib/jvm/temurin-22");
    }

    private void testInstall(String temurinReleaseName, String javaHome, String expectedJavaVersion)
    {
        String rpm = "/" + new File(rpmHostPath).getName();
        String command = """
                microdnf install -y tar gzip python sudo shadow-utils
                %s
                rpm -i %s
                mkdir /etc/trino/catalog
                echo CONFIG_ENV[HMS_PORT]=9083 >> /etc/trino/env.sh
                echo CONFIG_ENV[NODE_ID]=test-node-id-injected-via-env >> /etc/trino/env.sh
                sed -i "s/^node.id=.*/node.id=\\${ENV:NODE_ID}/g" /etc/trino/node.properties
                cat > /etc/trino/catalog/hive.properties <<"EOT"
                connector.name=hive
                hive.metastore.uri=thrift://localhost:${ENV:HMS_PORT}
                EOT
                cat > /etc/trino/catalog/jmx.properties <<"EOT"
                connector.name=jmx
                EOT
                /etc/init.d/trino start
                tail ---disable-inotify -F /var/log/trino/server.log
                """.formatted(installJavaCommand(temurinReleaseName, javaHome), rpm);

        try (GenericContainer<?> container = new GenericContainer<>(BASE_IMAGE)) {
            container.withExposedPorts(8080)
                    // the RPM is hundreds MB and file system bind is much more efficient
                    .withFileSystemBind(rpmHostPath, rpm, BindMode.READ_ONLY)
                    .withCommand("sh", "-xeuc", command)
                    .waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(5)))
                    .start();
            QueryRunner queryRunner = new QueryRunner(container.getHost(), container.getMappedPort(8080));
            assertThat(queryRunner.execute("SHOW CATALOGS")).isEqualTo(ImmutableSet.of(asList("system"), asList("hive"), asList("jmx")));
            assertThat(queryRunner.execute("SELECT node_id FROM system.runtime.nodes")).isEqualTo(ImmutableSet.of(asList("test-node-id-injected-via-env")));
            // TODO remove usage of assertEventually once https://github.com/trinodb/trino/issues/2214 is fixed
            assertEventually(
                    new io.airlift.units.Duration(1, MINUTES),
                    () -> assertThat(queryRunner.execute("SELECT specversion FROM jmx.current.\"java.lang:type=runtime\"")).isEqualTo(ImmutableSet.of(asList(expectedJavaVersion))));
        }
    }


    private void testUninstall(String temurinReleaseName, String javaHome)
            throws Exception
    {
        String rpm = "/" + new File(rpmHostPath).getName();
        String installAndStartTrino = """
                microdnf install -y tar gzip python sudo shadow-utils
                %s
                rpm -i %s
                /etc/init.d/trino start
                tail ---disable-inotify -F /var/log/trino/server.log
                """.formatted(installJavaCommand(temurinReleaseName, javaHome), rpm);

        try (GenericContainer<?> container = new GenericContainer<>(BASE_IMAGE)) {
            container.withFileSystemBind(rpmHostPath, rpm, BindMode.READ_ONLY)
                    .withCommand("sh", "-xeuc", installAndStartTrino)
                    .waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(5)))
                    .start();
            String uninstallTrino = """
                    /etc/init.d/trino stop
                    rpm -e trino-server-rpm
                    """;
            container.execInContainer("sh", "-xeuc", uninstallTrino);

            ExecResult actual = container.execInContainer("rpm", "-q", "trino-server-rpm");
            assertThat(actual.getStdout()).isEqualTo("package trino-server-rpm is not installed\n");

            assertPathDeleted(container, "/var/lib/trino");
            assertPathDeleted(container, "/usr/lib/trino");
            assertPathDeleted(container, "/etc/init.d/trino");
            assertPathDeleted(container, "/usr/shared/doc/trino");
        }
    }

    @Test
    public void testRpmContents()
            throws IOException, InterruptedException
    {
        String rpm = "/" + new File(rpmHostPath).getName();
        try (GenericContainer<?> container = new GenericContainer<>(BASE_IMAGE)) {
            container
                    .withFileSystemBind(rpmHostPath, rpm, BindMode.READ_ONLY)
                    .withCommand("sleep 1h")
                    .start();

            Map<String, PathInfo> files = listRpmFiles(container, rpm);

            // Launcher and init.d scripts
            assertThatPaths(files)
                    .exists("/usr/lib/trino/bin")
                    .path("/usr/lib/trino/bin/launcher").isOwnerExecutable()
                    .path("/usr/lib/trino/bin/launcher.py").isOwnerExecutable()
                    .path("/etc/init.d/trino").isOwnerExecutable()
                    .exists("/usr/lib/trino/bin/launcher.properties");

            // Configuration files
            assertThatPaths(files)
                    .path("/usr/lib/trino/etc").linksTo("/etc/trino")
                    .exists("/etc/trino/config.properties")
                    .exists("/etc/trino/jvm.config")
                    .exists("/etc/trino/env.sh")
                    .exists("/etc/trino/log.properties")
                    .exists("/etc/trino/node.properties");

            // Assemblies
            assertThatPaths(files)
                    .exists("/usr/lib/trino/shared")
                    .exists("/usr/shared/doc/trino")
                    .exists("/usr/shared/doc/trino/README.txt")
                    // Plugins' libs are always hardlinks
                    .paths("/usr/lib/trino/plugin/[a-z_]+\\.jar", path -> {
                        String filename = Path.of(path.getPath()).getFileName().toString();
                        path.isLink().linksTo("../../shared/" + filename);
                    });
        }
    }

    @Test
    public void testRpmMetadata()
            throws IOException, InterruptedException
    {
        String rpm = "/" + new File(rpmHostPath).getName();
        try (GenericContainer<?> container = new GenericContainer<>(BASE_IMAGE)) {
            container
                    .withFileSystemBind(rpmHostPath, rpm, BindMode.READ_ONLY)
                    .withCommand("sleep 1h")
                    .start();

            Map<String, String> rpmMetadata = getRpmMetadata(container, rpm);
            assertThat(rpmMetadata).extractingByKey("Name").isEqualTo("trino-server-rpm");
            assertThat(rpmMetadata).extractingByKey("Epoch").isEqualTo("0");
            assertThat(rpmMetadata).extractingByKey("Release").isEqualTo("1");
            assertThat(rpmMetadata).extractingByKey("Version").isEqualTo(getProjectVersion());
            assertThat(rpmMetadata).extractingByKey("Architecture").isEqualTo("noarch");
            assertThat(rpmMetadata).extractingByKey("License").isEqualTo("Apache License 2.0");
            assertThat(rpmMetadata).extractingByKey("Group").isEqualTo("Applications/Databases");
            assertThat(rpmMetadata).extractingByKey("URL").isEqualTo("https://trino.io");
        }
    }

    private static String temurinDownloadLink(String temurinReleaseName)
    {
        return switch (ARCH) {
            case AMD64 -> "https://api.adoptium.net/v3/binary/version/%s/linux/x64/jdk/hotspot/normal/eclipse?project=jdk".formatted(temurinReleaseName);
            case ARM64 -> "https://api.adoptium.net/v3/binary/version/%s/linux/aarch64/jdk/hotspot/normal/eclipse?project=jdk".formatted(temurinReleaseName);
            default -> throw new UnsupportedOperationException("Unsupported arch: " + ARCH);
        };
    }

    private static String installJavaCommand(String temurinReleaseName, String javaHome)
    {
        return """
                echo "Downloading JDK from %1$s"
                mkdir -p "%2$s"
                curl -#LfS "%1$s" | tar -zx --strip 1 -C "%2$s"
                """.formatted(temurinDownloadLink(temurinReleaseName), javaHome);
    }

    private static void assertPathDeleted(GenericContainer<?> container, String path)
            throws Exception
    {
        ExecResult actualResult = container.execInContainer(
                "sh",
                "-xeuc",
                format("test -d %s && echo -n 'path exists' || echo -n 'path deleted'", path));
        assertThat(actualResult.getStdout()).isEqualTo("path deleted");
        assertThat(actualResult.getExitCode()).isEqualTo(0);
    }

    private static Map<String, PathInfo> listRpmFiles(GenericContainer<?> container, String rpm)
            throws IOException, InterruptedException
    {
        ExecResult result = container.execInContainer("rpm", "-qlpv", rpm);
        assertThat(result.getExitCode()).isEqualTo(0);
        String lines = result.getStdout();

        // Parses RPM contents listing in the following format:
        // rw-r--r-- 1 root root 39 Feb 16 10:29 /usr/lib/trino/lib/jetty-alpn-client-11.0.20.jar -> ../shared/jetty-alpn-client-11.0.20.jar
        Splitter splitter = Splitter.onPattern("\s+").trimResults();
        ImmutableMap.Builder<String, PathInfo> builder = ImmutableMap.builder();
        for (String line : Splitter.on("\n").split(lines.trim())) {
            List<String> columns = splitter.splitToList(line);
            String mode = columns.get(0);
            boolean isLink = mode.startsWith("l");
            if (isLink) {
                assertThat(columns).hasSize(11);
            }
            else {
                assertThat(columns).hasSize(9);
            }

            String owner = columns.get(2);
            String group = columns.get(3);
            String path = columns.get(8);

            builder.put(path, new PathInfo(
                    path,
                    PosixFilePermissions.fromString(mode.substring(1)),
                    owner,
                    group,
                    isLink ? Optional.of(columns.get(10)) : Optional.empty()));
        }
        return builder.buildOrThrow();
    }

    private static Map<String, String> getRpmMetadata(GenericContainer<?> container, String rpm)
            throws IOException, InterruptedException
    {
        ExecResult result = container.execInContainer("rpm", "-qip", rpm);
        assertThat(result.getExitCode()).isEqualTo(0);
        List<String> lines = Splitter.on("\n").splitToList(result.getStdout().trim());
        Splitter splitter = Splitter.on(":").trimResults().limit(2);
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builderWithExpectedSize(lines.size() - 2); // last two lines are not KV
        for (String line : lines) {
            List<String> columns = splitter.splitToList(line);
            if (columns.size() == 2) {
                builder.put(columns.get(0), columns.get(1));
            }
        }
        return builder.buildOrThrow();
    }

    private static class QueryRunner
    {
        private final String host;
        private final int port;

        private QueryRunner(String host, int port)
        {
            this.host = requireNonNull(host, "host is null");
            this.port = port;
        }

        public Set<List<String>> execute(String sql)
        {
            try (Connection connection = getConnection(format("jdbc:trino://%s:%s", host, port), "test", null);
                    Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    ImmutableSet.Builder<List<String>> rows = ImmutableSet.builder();
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    while (resultSet.next()) {
                        ImmutableList.Builder<String> row = ImmutableList.builder();
                        for (int column = 1; column <= columnCount; column++) {
                            row.add(resultSet.getString(column));
                        }
                        rows.add(row.build());
                    }
                    return rows.build();
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    record PathInfo(String path, Set<PosixFilePermission> permissions, String owner, String group, Optional<String> link) {}

    static class PathInfoAssert
            extends AbstractAssert<PathInfoAssert, PathInfo>
    {
        private final PathsAssert files;

        public PathInfoAssert(PathInfo actual, PathsAssert files)
        {
            super(actual, PathInfoAssert.class);
            this.files = requireNonNull(files, "files is null");
        }

        public PathInfoAssert isLink()
        {
            if (actual.link.isEmpty()) {
                failWithMessage("Expected %s to be a link", actual.path);
            }
            return this;
        }

        public PathInfoAssert isOwnerExecutable()
        {
            if (!actual.permissions.contains(OWNER_EXECUTE)) {
                failWithMessage("Expected %s to be owner executable", actual.path);
            }
            return this;
        }

        public PathInfoAssert isNotOwnerExecutable()
        {
            if (actual.permissions.contains(OWNER_EXECUTE)) {
                failWithMessage("Expected %s not to be executable", actual.path);
            }
            return this;
        }

        public PathInfoAssert linksTo(String target)
        {
            if (actual.link.isEmpty()) {
                failWithMessage("Expected %s to be a link", actual.path);
            }

            if (!actual.link.equals(Optional.of(target))) {
                failWithMessage("Expected %s to be linked to %s but was linked to %s", actual.path, target, actual.link.get());
            }
            return this;
        }

        public String getPath()
        {
            return actual.path();
        }

        public PathInfoAssert path(String path)
        {
            return files.path(path);
        }

        public PathsAssert exists(String path)
        {
            return files.exists(path);
        }

        static class PathsAssert
                extends AbstractAssert<PathsAssert, Map<String, PathInfo>>
        {
            protected PathsAssert(Map<String, PathInfo> actual)
            {
                super(actual, PathsAssert.class);
            }

            public PathInfoAssert path(String path)
            {
                return new PathInfoAssert(actual.get(path), this);
            }

            public PathsAssert exists(String path)
            {
                if (!actual.containsKey(path)) {
                    failWithMessage("Expected path %s to exists", path);
                }

                return this;
            }

            public PathsAssert paths(String pattern, Consumer<PathInfoAssert> assertConsumer)
            {
                for (String path : actual.keySet()) {
                    if (path.matches(pattern)) {
                        assertConsumer.accept(path(path));
                    }
                }
                return this;
            }
        }

        static PathsAssert assertThatPaths(Map<String, PathInfo> paths)
        {
            return new PathsAssert(paths);
        }
    }
}
