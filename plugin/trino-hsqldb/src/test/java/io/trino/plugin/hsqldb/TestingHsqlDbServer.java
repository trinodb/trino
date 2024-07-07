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
package io.trino.plugin.hsqldb;

import io.trino.testing.ResourcePresence;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TestingHsqlDbServer
        implements AutoCloseable
{
    public static final String DEFAULT_VERSION = "2.7.3";
    private static final String HSQLDB_ARCHIVE = "hsqldb-%s.jar";
    private static final String DOWNLOAD_LOCATION = "https://repo1.maven.org/maven2/org/hsqldb/hsqldb/%s/%s";
    private static final int HSQLDB_PORT = 9001;
    private static class HsqldbContainer
            extends GenericContainer<HsqldbContainer>
    {
        private HsqldbContainer(ImageFromDockerfile image) {
            super(image);
        }
    }
    private final HsqldbContainer container;

    public TestingHsqlDbServer()
    {
        this(DEFAULT_VERSION);
    }

    public TestingHsqlDbServer(String tag)
    {
        String archive = String.format(HSQLDB_ARCHIVE, tag);
        String location = String.format(DOWNLOAD_LOCATION, tag, archive);
        ImageFromDockerfile image = new ImageFromDockerfile()
                                                .withDockerfileFromBuilder(builder ->
                                                        builder
                                                                .from("adoptopenjdk/openjdk11")
                                                                .add(location, archive)
                                                                .expose(HSQLDB_PORT)
                                                                .cmd("java", "-cp",
                                                                        archive, "org.hsqldb.server.Server",
                                                                        "--port", String.valueOf(HSQLDB_PORT))
                                                                .build());
        container = new HsqldbContainer(image).withExposedPorts(HSQLDB_PORT);
        container.start();
    }

    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getUsername()
    {
        return "SA";
    }

    public String getPassword()
    {
        return "";
    }

    public String getJdbcUrl()
    {
        String url = String.format("jdbc:hsqldb:hsql://localhost:%s/", container.getMappedPort(HSQLDB_PORT));
        System.out.println(url);
        return url;
    }

    @Override
    public void close()
    {
        container.close();
    }

    @ResourcePresence
    public boolean isRunning()
    {
         return container.isRunning();
    }
}
