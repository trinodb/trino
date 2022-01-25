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

import com.clickhouse.client.ClickHouseVersion;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static java.lang.String.format;
import static org.testcontainers.containers.ClickHouseContainer.HTTP_PORT;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public class TestingClickHouseServer
        implements Closeable
{
    private static final DockerImageName CLICKHOUSE_IMAGE = DockerImageName.parse("yandex/clickhouse-server");
    public static final DockerImageName CLICKHOUSE_LATEST_IMAGE = CLICKHOUSE_IMAGE.withTag("21.11.10.1");
    public static final DockerImageName CLICKHOUSE_DEFAULT_IMAGE = CLICKHOUSE_IMAGE.withTag("21.3.2.5"); // EOL is 30 Mar 2022

    private static final String CLICKHOUSE_LATEST_DRIVER_CLASS_NAME = "com.clickhouse.jdbc.ClickHouseDriver";
    // TODO: This Driver will not be available when clickhouse-jdbc is upgraded to 0.4.0 or above
    private static final String CLICKHOUSE_DEPRECATED_DRIVER_CLASS_NAME = "ru.yandex.clickhouse.ClickHouseDriver";
    private static final String CLICKHOUSE_LATEST_DRIVER_MINIMUM_SUPPORTED_VERSION = "20.7";

    // Altinity Stable Builds Life-Cycle Table https://docs.altinity.com/altinitystablebuilds/#altinity-stable-builds-life-cycle-table
    private static final DockerImageName ALTINITY_IMAGE = DockerImageName.parse("altinity/clickhouse-server").asCompatibleSubstituteFor("yandex/clickhouse-server");
    public static final DockerImageName ALTINITY_LATEST_IMAGE = ALTINITY_IMAGE.withTag("21.8.13.1.altinitystable");
    public static final DockerImageName ALTINITY_DEFAULT_IMAGE = ALTINITY_IMAGE.withTag("20.3.12-aes"); // EOL is 24 June 2022

    private final ClickHouseContainer dockerContainer;
    private final boolean latestDriverMinimumSupportedVersion;

    public TestingClickHouseServer()
    {
        this(CLICKHOUSE_DEFAULT_IMAGE);
    }

    public TestingClickHouseServer(DockerImageName image)
    {
        latestDriverMinimumSupportedVersion = ClickHouseVersion.of(image.getVersionPart()).isNewerOrEqualTo(CLICKHOUSE_LATEST_DRIVER_MINIMUM_SUPPORTED_VERSION);
        dockerContainer = (ClickHouseContainer) createContainer(latestDriverMinimumSupportedVersion, image)
                .withCopyFileToContainer(forClasspathResource("custom.xml"), "/etc/clickhouse-server/config.d/custom.xml")
                .withStartupAttempts(10);

        dockerContainer.start();
    }

    private static ClickHouseContainer createContainer(boolean latestDriverMinimumSupportedVersion, DockerImageName image)
    {
        String driverClass = getClickhouseDriverClassName(latestDriverMinimumSupportedVersion);
        return new ClickHouseContainer(image)
        {
            @Override
            public String getDriverClassName()
            {
                return driverClass;
            }
        };
    }

    private static String getClickhouseDriverClassName(boolean latestDriverMinimumSupportedVersion)
    {
        if (latestDriverMinimumSupportedVersion) {
            return CLICKHOUSE_LATEST_DRIVER_CLASS_NAME;
        }

        return CLICKHOUSE_DEPRECATED_DRIVER_CLASS_NAME;
    }

    public boolean isLatestDriverMinimumSupportedVersion()
    {
        return latestDriverMinimumSupportedVersion;
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
        return format("jdbc:clickhouse://%s:%s/", dockerContainer.getContainerIpAddress(),
                dockerContainer.getMappedPort(HTTP_PORT));
    }

    @Override
    public void close()
    {
        dockerContainer.stop();
    }
}
