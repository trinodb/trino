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
package io.trino.plugin.databend;

import org.testcontainers.databend.DatabendContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

public class TestingDatabendServer
        implements Closeable
{
    private static final DockerImageName DATABEND_IMAGE = DockerImageName.parse("datafuselabs/databend:nightly");
    private static final String QUERY_CONFIG_PATH = "/databend-config/query.toml";

    private final DatabendContainer databendContainer;
    private final String jdbcUrl;

    public TestingDatabendServer()
    {
        DatabendContainer started = null;
        String readyJdbcUrl = null;
        boolean success = false;
        try {
            Path configFile = createConfigFile(buildDatabendConfig());

            started = new DatabendContainer(DATABEND_IMAGE)
                    .withEnv("QUERY_DEFAULT_USER", getUser())
                    .withEnv("QUERY_DEFAULT_PASSWORD", getPassword())
                    .withEnv("QUERY_CONFIG_FILE", QUERY_CONFIG_PATH)
                    .withCopyFileToContainer(MountableFile.forHostPath(configFile), QUERY_CONFIG_PATH);

            started.start();
            readyJdbcUrl = ensureReadyAndGetJdbcUrl(started);
            success = true;
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to prepare Databend config", e);
        }
        finally {
            if (!success) {
                closeQuietly(started);
            }
        }

        this.databendContainer = started;
        this.jdbcUrl = readyJdbcUrl;
    }

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    public String getUser()
    {
        return "root";
    }

    public String getPassword()
    {
        return "";
    }

    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, getUser(), getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    @Override
    public void close()
    {
        closeQuietly(databendContainer);
    }

    private String ensureReadyAndGetJdbcUrl(DatabendContainer container)
    {
        String url = withPresignDisabled(container.getJdbcUrl());
        requireNonNull(url, "Databend JDBC URL is null");

        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();

        while (true) {
            try (Connection connection = DriverManager.getConnection(url, getUser(), getPassword());
                    Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1");
                statement.execute("CREATE DATABASE IF NOT EXISTS default");
                return url;
            }
            catch (SQLException ex) {
                if (System.nanoTime() > deadline) {
                    throw new RuntimeException("Databend server did not become ready in time", ex);
                }
                try {
                    Thread.sleep(2_000);
                }
                catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for Databend to start", interruptedException);
                }
            }
        }
    }

    private static void closeQuietly(DatabendContainer container)
    {
        if (container != null) {
            try {
                container.close();
            }
            catch (RuntimeException ignored) {
            }
        }
    }

    private static Path createConfigFile(String contents)
            throws IOException
    {
        Path configFile = Files.createTempFile("databend-config", ".toml");
        Files.writeString(configFile, contents, StandardCharsets.UTF_8);
        configFile.toFile().deleteOnExit();
        return configFile;
    }

    private static String buildDatabendConfig()
    {
        return String.join("\n",
                "[query]",
                "max_active_sessions = 256",
                "shutdown_wait_timeout_ms = 5000",
                "",
                "flight_api_address = \"0.0.0.0:9090\"",
                "admin_api_address = \"0.0.0.0:8080\"",
                "metric_api_address = \"0.0.0.0:7070\"",
                "",
                "mysql_handler_host = \"0.0.0.0\"",
                "mysql_handler_port = 3307",
                "",
                "clickhouse_http_handler_host = \"0.0.0.0\"",
                "clickhouse_http_handler_port = 8124",
                "",
                "http_handler_host = \"0.0.0.0\"",
                "http_handler_port = 8000",
                "",
                "flight_sql_handler_host = \"0.0.0.0\"",
                "flight_sql_handler_port = 8900",
                "",
                "tenant_id = \"default\"",
                "cluster_id = \"default\"",
                "",
                "[log]",
                "",
                "[log.stderr]",
                "level = \"WARN\"",
                "format = \"text\"",
                "",
                "[log.file]",
                "level = \"INFO\"",
                "dir = \"/var/log/databend\"",
                "",
                "[meta]",
                "endpoints = [\"0.0.0.0:9191\"]",
                "username = \"root\"",
                "password = \"root\"",
                "client_timeout_in_second = 60",
                "",
                "[[query.users]]",
                "name = \"databend\"",
                "auth_type = \"double_sha1_password\"",
                "auth_string = \"3081f32caef285c232d066033c89a78d88a6d8a5\"",
                "",
                "[[query.users]]",
                "name = \"root\"",
                "auth_type = \"no_password\"",
                "",
                "[storage]",
                "type = \"fs\"",
                "",
                "[storage.fs]",
                "data_path = \"/var/lib/databend/data\"",
                "") + "\n";
    }

    private static String withPresignDisabled(String jdbcUrl)
    {
        if (jdbcUrl.contains("?")) {
            return jdbcUrl + "&presigned_url_disabled=true";
        }
        return jdbcUrl + "?presigned_url_disabled=true";
    }
}
