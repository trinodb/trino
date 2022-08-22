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
package io.trino.plugin.postgresql;

import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

// TODO https://github.com/trinodb/trino/issues/13771 Move to CockroachDB connector
public class TestingCockroachServer
        implements TestingPostgreSql
{
    private final CockroachContainer dockerContainer;

    public TestingCockroachServer()
    {
        dockerContainer = new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v21.2.14"));
        dockerContainer.start();

        execute("CREATE SCHEMA tpch"); // CockroachContainer.withDatabaseName is unsupported
    }

    @Override
    public String getJdbcUrl()
    {
        return dockerContainer.getJdbcUrl();
    }

    @Override
    public String getUser()
    {
        return dockerContainer.getUsername();
    }

    @Override
    public String getPassword()
    {
        return dockerContainer.getPassword();
    }

    @Override
    public void close()
            throws IOException
    {
        dockerContainer.close();
    }

    @Override
    public void execute(@Language("SQL") String sql)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), dockerContainer.getUsername(), dockerContainer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
