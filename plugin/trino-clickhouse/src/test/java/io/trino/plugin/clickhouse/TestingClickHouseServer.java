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
package io.trino.plugin.clickhouse;

import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.shaded.com.google.common.collect.Sets;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Set;

import static java.lang.String.format;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public class TestingClickHouseServer
        extends ClickHouseContainer
        implements Closeable
{
    private static final DockerImageName CLICKHOUSE_IMAGE = DockerImageName.parse("yandex/clickhouse-server");
    public static final DockerImageName CLICKHOUSE_LATEST_IMAGE = CLICKHOUSE_IMAGE.withTag("21.11.10.1");
    public static final DockerImageName CLICKHOUSE_DEFAULT_IMAGE = CLICKHOUSE_IMAGE.withTag("21.3.2.5"); // EOL is 30 Mar 2022

    // Altinity Stable Builds Life-Cycle Table https://docs.altinity.com/altinitystablebuilds/#altinity-stable-builds-life-cycle-table
    private static final DockerImageName ALTINITY_IMAGE = DockerImageName.parse("altinity/clickhouse-server").asCompatibleSubstituteFor("yandex/clickhouse-server");
    public static final DockerImageName ALTINITY_LATEST_IMAGE = ALTINITY_IMAGE.withTag("21.8.13.1.altinitystable");
    public static final DockerImageName ALTINITY_DEFAULT_IMAGE = ALTINITY_IMAGE.withTag("20.3.12-aes"); // EOL is 24 June 2022

    private static final SecureRandom RANDOM = new SecureRandom();
    private int port;

    public TestingClickHouseServer()
    {
        this(CLICKHOUSE_DEFAULT_IMAGE);
    }

    public TestingClickHouseServer(DockerImageName image)
    {
        super(image);
        port = RANDOM.nextInt();
        super.withCopyFileToContainer(forClasspathResource("custom.xml"), "/etc/clickhouse-server/config.d/custom.xml");
        super.withStartupAttempts(10);
        start();
    }

    private static Path generateConfig(int port)
            throws IOException
    {
        File tempFile = File.createTempFile("custom-", ".xml");
        String string = format("" +
                "<?xml version=\"1.0\"?>\n" +
                "<yandex>\n" +
                "    <!-- To avoid \"failed to response\" error message during copy of TPCH tables -->\n" +
                "    <keep_alive_timeout>10</keep_alive_timeout>\n" +
                "    <http_port>%s</http_port>\n" +
                "</yandex>\n", port);
        Files.writeString(tempFile.toPath(), string);
        return tempFile.toPath();
    }

    @Override
    public GenericContainer<?> withExposedPorts(Integer... ports)
    {
        return super.withExposedPorts(9999);
    }

    @Override
    public Set<Integer> getLivenessCheckPortNumbers() {
        return Sets.newHashSet(new Integer[]{9999});
    }

    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    @Override
    public String getJdbcUrl()
    {
        return format("jdbc:clickhouse://%s:%s/", getContainerIpAddress(), getMappedPort(9999));
    }

    @Override
    public void close()
    {
        stop();
    }
}
