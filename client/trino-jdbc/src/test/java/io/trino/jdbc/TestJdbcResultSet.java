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
package io.trino.jdbc;

import io.airlift.log.Logging;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * An integration test for JDBC client interacting with Trino server.
 */
@TestInstance(PER_CLASS)
public class TestJdbcResultSet
        extends BaseTestJdbcResultSet
{
    private TestingTrinoServer server;

    @BeforeAll
    public void setupServer()
    {
        Logging.initialize();
        server = createTestingServer();
    }

    protected TestingTrinoServer createTestingServer()
    {
        return TestingTrinoServer.create();
    }

    @AfterAll
    public void tearDownServer()
            throws Exception
    {
        server.close();
        server = null;
    }

    @Override
    protected Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:trino://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }
}
