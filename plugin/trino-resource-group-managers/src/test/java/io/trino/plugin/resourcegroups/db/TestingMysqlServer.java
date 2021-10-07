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
package io.trino.plugin.resourcegroups.db;

import org.testcontainers.containers.MySQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TestingMysqlServer
        extends MySQLContainer<TestingMysqlServer>
{
    public TestingMysqlServer()
    {
        this("mysql:8.0.12");
    }

    public TestingMysqlServer(String dockerImageName)
    {
        super(dockerImageName);
    }

    public void executeSql(String sql)
    {
        try (Connection connection = createConnection("");
                Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
