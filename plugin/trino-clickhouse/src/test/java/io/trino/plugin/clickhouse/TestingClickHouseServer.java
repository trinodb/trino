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

import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static java.lang.String.format;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public class TestingClickHouseServer
        implements Closeable
{
    private static final DockerImageName CLICKHOUSE_IMAGE = DockerImageName.parse("clickhouse/clickhouse-server");
    public static final DockerImageName CLICKHOUSE_LATEST_IMAGE = CLICKHOUSE_IMAGE.withTag("24.1.8.22");   // EOL by Apr 2025
    public static final DockerImageName CLICKHOUSE_DEFAULT_IMAGE = CLICKHOUSE_IMAGE.withTag("23.8.12.13"); // EOL by Jun 2024

    // Altinity Stable Builds Life-Cycle Table https://docs.altinity.com/altinitystablebuilds/#altinity-stable-builds-life-cycle-table
    // On Mac/arm try `21.8.12.29.altinitydev.arm` instead of the specified stable build
    private static final DockerImageName ALTINITY_IMAGE = DockerImageName.parse("altinity/clickhouse-server").asCompatibleSubstituteFor("clickhouse/clickhouse-server");
    public static final DockerImageName ALTINITY_LATEST_IMAGE = ALTINITY_IMAGE.withTag("23.8.8.21.altinitystable");  // EOL is 27 Dec 2026
    public static final DockerImageName ALTINITY_DEFAULT_IMAGE = ALTINITY_IMAGE.withTag("21.8.15.15.altinitystable"); // EOL is 30 Aug 2024

    private final ClickHouseContainer dockerContainer;

    public TestingClickHouseServer()
    {
        this(CLICKHOUSE_DEFAULT_IMAGE);
    }

    public TestingClickHouseServer(DockerImageName image)
    {
        dockerContainer = new ClickHouseContainer(image)
                .withCopyFileToContainer(forClasspathResource("custom.xml"), "/etc/clickhouse-server/config.d/custom.xml")
                .withStartupAttempts(10);

        dockerContainer.start();
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

    public String getJdbcUrl()
    {
        return format("jdbc:clickhouse://%s:%s/", dockerContainer.getHost(),
                dockerContainer.getMappedPort(8123));
    }

    @Override
    public void close()
    {
        dockerContainer.stop();
    }
}
