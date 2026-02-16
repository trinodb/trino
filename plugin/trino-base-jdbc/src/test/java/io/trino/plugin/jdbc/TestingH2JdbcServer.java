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
package io.trino.plugin.jdbc;

import io.airlift.log.Logger;
import io.trino.testing.ResourcePresence;
import io.trino.testing.containers.PrintingLogConsumer;
import io.trino.testing.sql.JdbcSqlExecutor;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

final class TestingH2JdbcServer
        implements AutoCloseable
{
    private static final Logger log = Logger.get(TestingH2JdbcServer.class);

    public static final String TEST_SCHEMA = "PUBLIC";
    public static final String TEST_USER = "SA";
    public static final String TEST_PASSWORD = "";

    public static final String DEFAULT_VERSION = "2.4.240";
    public static final String LATEST_VERSION = "2.4.240";
    private static final String DEFAULT_JMV = "adoptopenjdk/openjdk11";
    private static final String H2DB_ARCHIVE = "h2-%s.jar";
    private static final String H2DB_URL = "jdbc:h2:tcp://%s:%s/./h2";
    private static final String DOWNLOAD_LOCATION = "https://repo1.maven.org/maven2/com/h2database/h2/%s/%s";
    private static final int H2DB_PORT = 9092;

    private static class H2Container
            extends GenericContainer<H2Container>
    {
        private H2Container(ImageFromDockerfile image)
        {
            super(image);
        }
    }

    private final H2Container container;

    public TestingH2JdbcServer()
    {
        this(DEFAULT_VERSION);
    }

    public TestingH2JdbcServer(String tag)
    {
        String archive = String.format(H2DB_ARCHIVE, tag);
        String location = String.format(DOWNLOAD_LOCATION, tag, archive);
        ImageFromDockerfile image = new ImageFromDockerfile()
                .withDockerfileFromBuilder(builder ->
                        builder
                                .from(DEFAULT_JMV)
                                .add(location, archive)
                                .expose(H2DB_PORT)
                                .cmd("java", "-cp",
                                        archive, "org.h2.tools.Server",
                                        "-tcp", "-tcpAllowOthers",
                                        "-tcpPort", Integer.toString(H2DB_PORT),
                                        "-ifNotExists")
                                .build());
        container = new H2Container(image).withExposedPorts(H2DB_PORT);
        container.start();
        container.followOutput(new PrintingLogConsumer("H2DB"));
        log.info("%s version %s listening on port: %s", TestingH2JdbcServer.class.getName(), tag, getJdbcUrl());
    }

    public JdbcSqlExecutor getSqlExecutor()
    {
        return new JdbcSqlExecutor(getJdbcUrl(), getProperties(getUsername(), getPassword()));
    }

    private static Properties getProperties(String user, String password)
    {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        return properties;
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
        return TEST_USER;
    }

    public String getPassword()
    {
        return TEST_PASSWORD;
    }

    public String getJdbcUrl()
    {
        return String.format(H2DB_URL, container.getHost(), container.getMappedPort(H2DB_PORT));
    }

    public Connection getConnection()
            throws SQLException
    {
        return DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
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
