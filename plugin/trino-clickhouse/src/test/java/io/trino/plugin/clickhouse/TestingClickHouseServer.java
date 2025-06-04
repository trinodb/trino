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

import static org.testcontainers.utility.MountableFile.forClasspathResource;

public class TestingClickHouseServer
        implements Closeable
{
    /**
     * <a href="https://clickhouse.com/docs/en/faq/operations/production#how-to-choose-between-clickhouse-releases">How to Choose Between ClickHouse Releases?</a>
     * <p>
     * stable is the kind of package we recommend by default.
     * They are released roughly monthly (and thus provide new features with reasonable delay)
     * and three latest stable releases are supported in terms of diagnostics and backporting of bugfixes.
     * lts are released twice a year and are supported for a year after their initial release.
     * <p>
     * <a href="https://kb.altinity.com/altinity-kb-setup-and-maintenance/clickhouse-versions/">Versioning schema</a>
     */
    private static final DockerImageName CLICKHOUSE_IMAGE = DockerImageName.parse("clickhouse/clickhouse-server");
    // https://clickhouse.com/docs/en/whats-new/changelog#-clickhouse-release-2412-2024-12-19
    public static final DockerImageName CLICKHOUSE_LATEST_IMAGE = CLICKHOUSE_IMAGE.withTag("24.12.1.1614");   // EOL in 3 releases after 2024-12-19
    // https://clickhouse.com/docs/en/whats-new/changelog#-clickhouse-release-243-lts-2024-03-27
    public static final DockerImageName CLICKHOUSE_DEFAULT_IMAGE = CLICKHOUSE_IMAGE.withTag("24.3.14.35"); // EOL in 1 year after 2024-03-27

    /**
     * <a href="https://docs.altinity.com/altinitystablebuilds/#altinity-stable-builds-life-cycle-table">Altinity Stable Builds Life-Cycle Table</a>
     * <p>
     * On Mac/arm 23.3.13.7.altinitystable, 23.8.8.21.altinitystable and 22.8.15.25.altinitystable and later versions available on ARM.
     */
    private static final DockerImageName ALTINITY_IMAGE = DockerImageName.parse("altinity/clickhouse-server").asCompatibleSubstituteFor("clickhouse/clickhouse-server");
    public static final DockerImageName ALTINITY_LATEST_IMAGE = ALTINITY_IMAGE.withTag("24.3.12.76.altinitystable");  // EOL is 23 Jul 2027
    public static final DockerImageName ALTINITY_DEFAULT_IMAGE = ALTINITY_IMAGE.withTag("22.3.15.34.altinitystable"); // EOL is 15 Jul 2025

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
        try (Connection connection = DriverManager.getConnection(dockerContainer.getJdbcUrl(), dockerContainer.getUsername(), dockerContainer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    public String getJdbcUrl()
    {
        return dockerContainer.getJdbcUrl();
    }

    public String getUsername()
    {
        return dockerContainer.getUsername();
    }

    public String getPassword()
    {
        return dockerContainer.getPassword();
    }

    @Override
    public void close()
    {
        dockerContainer.stop();
    }
}
