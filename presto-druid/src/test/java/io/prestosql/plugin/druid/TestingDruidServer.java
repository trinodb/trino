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
package io.prestosql.plugin.druid;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.testing.docker.DockerContainer;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import static java.lang.String.format;

public class TestingDruidServer
        implements Closeable
{
    private static final Logger log = Logger.get(TestingDruidServer.class);
    private static final int DRUID_BROKER_PORT = 8082;
    private static final int DRUID_OVERLORD_PORT = 8090;

    private final DockerContainer dockerContainer;

    public TestingDruidServer()
    {
        this.dockerContainer = new DockerContainer(
                // TODO: replace this with prestodb/druid0.14.0 once container is pushed under docker-images.
                "puneetjaiswal/containers:druid0.14.0",
                ImmutableList.of(DRUID_BROKER_PORT, DRUID_OVERLORD_PORT),
                ImmutableMap.of(),
                portProvider -> TestingDruidServer.execute(portProvider, "SELECT 1"));
    }

    public void execute(String sql)
    {
        execute(dockerContainer::getHostPort, sql);
    }

    private static void execute(DockerContainer.HostPortProvider hostPortProvider, String sql)
    {
        String jdbcUrl = getJdbcUrl(hostPortProvider.getHostPort(DRUID_BROKER_PORT));
        try (Connection conn = DriverManager.getConnection(jdbcUrl, new Properties());
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
        catch (Exception e) {
            log.error(e, "Failed to execute statement: " + sql);
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    public String getJdbcUrl()
    {
        return getJdbcUrl(dockerContainer.getHostPort(DRUID_BROKER_PORT));
    }

    private static String getJdbcUrl(int port)
    {
        return format("jdbc:avatica:remote:url=http://localhost:%s/druid/v2/sql/avatica/", port);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
