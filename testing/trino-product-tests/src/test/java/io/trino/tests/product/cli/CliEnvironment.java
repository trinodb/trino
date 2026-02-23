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
package io.trino.tests.product.cli;

import com.google.common.collect.ImmutableList;
import io.trino.testing.TestingProperties;
import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.tests.product.hive.HiveCatalogPropertiesBuilder;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * CLI product test environment with Trino configured with TPCH, memory, and PostgreSQL catalogs.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Hadoop container with Hive Metastore and HDFS for Hive-backed CLI tests</li>
 *   <li>Trino container with tpch, hive, memory, and postgresql catalogs</li>
 *   <li>CLI executable mounted at /docker/trino-cli</li>
 *   <li>Methods for executing CLI commands via docker exec</li>
 * </ul>
 */
public class CliEnvironment
        extends ProductTestEnvironment
{
    private static final String CLI_CONTAINER_PATH = "/docker/trino-cli";
    private static final String DEFAULT_USER = "test";

    private Network network;
    private HadoopContainer hadoop;
    private PostgreSQLContainer<?> postgresql;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        // Find CLI jar.
        Path cliJar = findCliJar();

        network = Network.newNetwork();

        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME);
        hadoop.start();

        postgresql = new PostgreSQLContainer<>("postgres:11")
                .withNetwork(network)
                .withNetworkAliases("postgresql")
                .withDatabaseName("test")
                .withUsername("test")
                .withPassword("test");
        postgresql.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
                .withCatalog("hive", HiveCatalogPropertiesBuilder.hiveCatalog(HiveCatalogPropertiesBuilder.hadoopMetastoreUri())
                        .withHadoopFileSystem()
                        .withCommonProperties()
                        .build())
                .withCatalog("memory", Map.of("connector.name", "memory"))
                .withCatalog("postgresql", Map.of(
                        "connector.name", "postgresql",
                        "connection-url", "jdbc:postgresql://postgresql:5432/test",
                        "connection-user", "test",
                        "connection-password", "test"))
                .build();

        // Copy CLI jar into container with executable permissions. Artifact downloads on CI do not
        // preserve the executable bit reliably, which breaks direct execution from bind mounts.
        trino.withCopyFileToContainer(MountableFile.forHostPath(cliJar, 0755), CLI_CONTAINER_PATH);

        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
            initializeHiveTpchTables();
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }

        // Create schemas used by tests.
        try (Connection conn = createTrinoConnection();
                var stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS hive.default");
            stmt.execute("CREATE SCHEMA IF NOT EXISTS memory.default");
            stmt.execute("CREATE SCHEMA IF NOT EXISTS postgresql.public");
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to create required schemas", e);
        }
    }

    private void initializeHiveTpchTables()
    {
        executeTrinoUpdate("""
                CREATE TABLE IF NOT EXISTS hive.default.nation AS
                SELECT
                    CAST(nationkey AS BIGINT) AS nationkey,
                    CAST(name AS VARCHAR(25)) AS name,
                    CAST(regionkey AS BIGINT) AS regionkey,
                    CAST(comment AS VARCHAR(152)) AS comment
                FROM tpch.tiny.nation
                """);
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino);
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user);
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino.getJdbcUrl();
    }

    @Override
    public boolean isRunning()
    {
        return trino != null && trino.isRunning();
    }

    @Override
    protected void doClose()
    {
        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (postgresql != null) {
            postgresql.close();
            postgresql = null;
        }
        if (hadoop != null) {
            hadoop.close();
            hadoop = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }

    private static Path findCliJar()
    {
        // Try to find CLI jar in expected location
        String version = TestingProperties.getProjectVersion();
        Path projectRoot = findProjectRoot();
        Path cliJar = projectRoot.resolve("client/trino-cli/target/trino-cli-" + version + "-executable.jar");

        if (Files.exists(cliJar)) {
            return cliJar;
        }

        throw new IllegalStateException(
                "CLI jar not found at " + cliJar + ". " +
                "Run 'mvn package -pl client/trino-cli -am -DskipTests' to build it.");
    }

    private static Path findProjectRoot()
    {
        // Walk up from current directory to find the root pom.xml
        // We identify root by presence of "client" directory which contains CLI module
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            Path clientDir = current.resolve("client");
            Path cliPom = clientDir.resolve("trino-cli/pom.xml");
            if (Files.exists(cliPom)) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Could not find project root (directory containing client/trino-cli/pom.xml). Current dir: " + System.getProperty("user.dir"));
    }

    // ==================== CLI Execution Methods ====================

    /**
     * Returns the Trino container for direct CLI execution.
     */
    public TrinoContainer getTrinoContainer()
    {
        return trino;
    }

    /**
     * Returns the internal Trino server URL (used by CLI inside container).
     */
    public String getInternalServerUrl()
    {
        return "http://localhost:8080";
    }

    /**
     * Returns the default user for CLI operations.
     */
    public String getDefaultUser()
    {
        return DEFAULT_USER;
    }

    /**
     * Executes a CLI command with the given arguments.
     * The server URL and user are automatically added.
     */
    public ExecResult executeCli(String... args)
            throws IOException, InterruptedException
    {
        return executeCliAsUser(DEFAULT_USER, args);
    }

    /**
     * Executes a CLI command with the given user and arguments.
     * The server URL is automatically added.
     */
    public ExecResult executeCliAsUser(String user, String... args)
            throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-jar");
        command.add(CLI_CONTAINER_PATH);
        command.add("--server");
        command.add(getInternalServerUrl());
        command.add("--user");
        command.add(user);

        for (String arg : args) {
            command.add(arg);
        }

        return trino.execInContainer(command.toArray(new String[0]));
    }

    /**
     * Executes a CLI command with custom environment variables.
     */
    public ExecResult executeCliWithEnv(Map<String, String> envVars, String... args)
            throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-jar");
        command.add(CLI_CONTAINER_PATH);
        command.add("--server");
        command.add(getInternalServerUrl());
        command.add("--user");
        command.add(DEFAULT_USER);

        for (String arg : args) {
            command.add(arg);
        }

        // Build environment variable prefix
        StringBuilder envPrefix = new StringBuilder();
        for (Map.Entry<String, String> entry : envVars.entrySet()) {
            envPrefix.append(entry.getKey())
                    .append("='")
                    .append(escapeForBash(entry.getValue()))
                    .append("' ");
        }

        // Execute via bash with environment variables
        return trino.execInContainer(
                ImmutableList.<String>builder()
                        .add("/bin/bash", "-c")
                        .add(envPrefix + String.join(" ", escapeCommand(command)))
                        .build()
                        .toArray(new String[0]));
    }

    /**
     * Executes a CLI command with stdin redirected from a string.
     */
    public ExecResult executeCliWithStdin(String stdinContent, String... args)
            throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-jar");
        command.add(CLI_CONTAINER_PATH);
        command.add("--server");
        command.add(getInternalServerUrl());
        command.add("--user");
        command.add(DEFAULT_USER);

        for (String arg : args) {
            command.add(arg);
        }

        // Use heredoc to pipe multi-line content to CLI
        // The 'TRINOSQL' marker is unlikely to appear in actual SQL
        String shellCommand = String.join(" ", escapeCommand(command)) + " <<'TRINOSQL'\n" + stdinContent + "\nTRINOSQL";

        return trino.execInContainer("/bin/bash", "-c", shellCommand);
    }

    /**
     * Executes a CLI command with --file argument pointing to a file in the container.
     */
    public ExecResult executeCliWithFile(String filePath, String... additionalArgs)
            throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-jar");
        command.add(CLI_CONTAINER_PATH);
        command.add("--server");
        command.add(getInternalServerUrl());
        command.add("--user");
        command.add(DEFAULT_USER);
        command.add("--file");
        command.add(filePath);

        for (String arg : additionalArgs) {
            command.add(arg);
        }

        return trino.execInContainer(command.toArray(new String[0]));
    }

    private List<String> escapeCommand(List<String> command)
    {
        return command.stream()
                .map(this::escapeForBash)
                .toList();
    }

    private String escapeForBash(String value)
    {
        // Escape single quotes by ending the quoted string, adding escaped quote, and starting new quoted string
        return "'" + value.replace("'", "'\\''") + "'";
    }
}
