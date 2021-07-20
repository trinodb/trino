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
package io.trino.plugin.sybase;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TestingSybaseServer
        extends JdbcDatabaseContainer<TestingSybaseServer>
{
    public static final String DEFAULT_TAG = "datagrip/sybase";
    private static final int SYBASE_PORT = 5000;

    public TestingSybaseServer()
    {
        this(DEFAULT_TAG);
    }

    public TestingSybaseServer(String dockerImageName)
    {
        super(DockerImageName.parse(dockerImageName));
        start();
    }

    @Override
    public String getDriverClassName()
    {
        return "net.sourceforge.jtds.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl()
    {
        return "jdbc:jtds:sybase://" + getContainerIpAddress() + ":" + getMappedPort(SYBASE_PORT) + "/testdb";
    }

    @Override
    public String getUsername()
    {
        return "sa";
    }

    @Override
    public String getPassword()
    {
        return "myPassword";
    }

    @Override
    protected String getTestQueryString()
    {
        return "SELECT 1";
    }

    public void execute(String sql)
    {
        execute(sql, getUsername(), getPassword());
    }

    public void execute(String sql, String user, String password)
    {
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), user, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
