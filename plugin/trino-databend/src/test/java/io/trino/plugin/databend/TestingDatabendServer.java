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

import io.trino.testing.ResourcePresence;
import org.testcontainers.databend.DatabendContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class TestingDatabendServer
        implements Closeable
{
    private static final DockerImageName DATABEND_IMAGE = DockerImageName.parse("datafuselabs/databend");
    public static final DockerImageName DATABEND_DEFAULT_IMAGE = DATABEND_IMAGE.withTag("v1.2.615");

    private final DatabendContainer dockerContainer;

    public TestingDatabendServer()
    {
        this(DATABEND_DEFAULT_IMAGE);
    }

    public TestingDatabendServer(DockerImageName image)
    {
        dockerContainer = new DatabendContainer(image).withUrlParam("presigned_url_disabled", "true");

        dockerContainer.start();
    }

    public Connection createConnection()
            throws SQLException
    {
        return DriverManager.getConnection(getJdbcUrl());
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
        // The databend testContainer has something wrong and the getJdbcUrl can't work correctly.
        return format("jdbc:databend://%s:%s@%s:%s/",
                dockerContainer.getUsername(),
                dockerContainer.getPassword(),
                dockerContainer.getHost(),
                dockerContainer.getMappedPort(8000));
    }

    @Override
    public void close()
    {
        dockerContainer.stop();
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return dockerContainer.getContainerId() != null;
    }
}
