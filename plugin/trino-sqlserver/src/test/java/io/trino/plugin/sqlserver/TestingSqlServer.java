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
package io.trino.plugin.sqlserver;

import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;

import static java.lang.String.format;

public final class TestingSqlServer
        extends MSSQLServerContainer<TestingSqlServer>
{
    private final boolean snapshotIsolationEnabled;
    private final String databaseName;

    public TestingSqlServer()
    {
        this(true);
    }

    public TestingSqlServer(boolean snapshotIsolationEnabled)
    {
        super(DockerImageName.parse("microsoft/mssql-server-linux:2017-CU13").asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server:2017-CU12"));
        this.snapshotIsolationEnabled = snapshotIsolationEnabled;
        this.databaseName = "database_" + UUID.randomUUID().toString().replace("-", "");

        addEnv("ACCEPT_EULA", "yes");
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

    @Override
    public void start()
    {
        super.start();
        setUpDatabase();
    }

    private void setUpDatabase()
    {
        execute("CREATE DATABASE " + databaseName);

        if (snapshotIsolationEnabled) {
            execute(format("ALTER DATABASE %s SET READ_COMMITTED_SNAPSHOT ON", databaseName));
            execute(format("ALTER DATABASE %s SET ALLOW_SNAPSHOT_ISOLATION ON", databaseName));
        }

        this.withUrlParam("database", this.databaseName);
    }
}
