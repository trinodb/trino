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
package io.prestosql.plugin.sqlserver;

import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.Connection;
import java.sql.Statement;

public final class TestingSqlServer
        extends MSSQLServerContainer<TestingSqlServer>
{
    public TestingSqlServer()
    {
        super("microsoft/mssql-server-linux:2017-CU13");
    }

    public void execute(String sql)
    {
        try (Connection connection = createConnection("");
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }
}
