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
import okhttp3.OkHttpClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestNonRegisteringTrinoDriver
{
    private TestingTrinoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = TestingTrinoServer.create();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        server.close();
        server = null;
    }

    @Test
    public void testDriverWithCustomOkHttpClient()
            throws Exception
    {
        AtomicBoolean customClientInvoked = new AtomicBoolean(false);

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(chain -> {
                    customClientInvoked.getAndSet(true);
                    return chain.proceed(chain.request());
                })
                .build();

        NonRegisteringTrinoDriver driverWithCustomHttpClient = new NonRegisteringTrinoDriver(client);
        DriverManager.registerDriver(driverWithCustomHttpClient);

        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.executeQuery("SELECT 123");
                assertTrue(customClientInvoked.get());
            }
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:trino://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }
}
