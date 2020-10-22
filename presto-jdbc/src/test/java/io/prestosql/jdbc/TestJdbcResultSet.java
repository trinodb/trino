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
package io.prestosql.jdbc;

import io.airlift.log.Logging;
import io.prestosql.server.testing.TestingPrestoServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.lang.String.format;

public class TestJdbcResultSet
        extends BaseTestJdbcResultSet
{
    private TestingPrestoServer server;

    @BeforeClass
    public void setupServer()
    {
        Logging.initialize();
        server = TestingPrestoServer.create();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        server.close();
    }

    @Override
    protected Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }

    @Override
    protected int getTestedPrestoServerVersion()
    {
        // Lastest version
        return Integer.MAX_VALUE;
    }
}
