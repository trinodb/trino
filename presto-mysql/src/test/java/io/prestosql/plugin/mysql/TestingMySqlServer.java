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
package io.prestosql.plugin.mysql;

import org.testcontainers.containers.MySQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class TestingMySqlServer
        extends MySQLContainer<TestingMySqlServer>
{
    public TestingMySqlServer()
    {
        this(false);
    }

    public TestingMySqlServer(boolean globalTransactionEnable)
    {
        this("mysql:8.0.12", globalTransactionEnable);
    }

    public TestingMySqlServer(String dockerImageName, boolean globalTransactionEnable)
    {
        super(dockerImageName);
        withDatabaseName("tpch");
        if (globalTransactionEnable) {
            withCommand("--gtid-mode=ON", "--enforce-gtid-consistency=ON");
        }
        start();
        execute(format("GRANT ALL PRIVILEGES ON *.* TO '%s'", getUsername()), "root", getPassword());
    }

    @Override
    public String getJdbcUrl()
    {
        return format("jdbc:mysql://%s:%s?useSSL=false&allowPublicKeyRetrieval=true", getContainerIpAddress(), getMappedPort(MYSQL_PORT));
    }

    public void execute(String sql)
    {
        execute(sql, getUsername(), getPassword());
    }

    public void execute(String sql, String user, String password)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), user, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
