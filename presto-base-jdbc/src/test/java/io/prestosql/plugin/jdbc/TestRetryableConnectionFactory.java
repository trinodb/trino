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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.prestosql.spi.testing.InterfaceTestUtils;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestRetryableConnectionFactory
{
    @Test
    public void testEverythingImplemented()
    {
        InterfaceTestUtils.assertAllMethodsOverridden(ConnectionFactory.class, RetryableConnectionFactory.class);
    }

    @Test
    public void testRetryConnection() throws SQLException
    {
        TestConnectionFactory delegate = new TestConnectionFactory(2);
        RetryJdbcConfig config = new RetryJdbcConfig();

        // Max retries = 3 by default
        ConnectionFactory connectionFactory = new RetryableConnectionFactory(delegate, config);

        Connection conn = connectionFactory.openConnection(new JdbcIdentity("test", Optional.empty(), ImmutableMap.of()));

        assertNotNull(conn);
        assertEquals(delegate.counter, 3);
    }

    @Test(expectedExceptions = SQLException.class)
    public void testGiveUpToRetryConnection() throws SQLException
    {
        TestConnectionFactory delegate = new TestConnectionFactory(3);

        // Set max retries = 2
        ConnectionFactory connectionFactory = new RetryableConnectionFactory(delegate, Duration.valueOf("1s"), Duration.valueOf("1m"), 2, 2.0);

        Connection conn = connectionFactory.openConnection(new JdbcIdentity("test", Optional.empty(), ImmutableMap.of()));
    }

    private static class TestConnectionFactory
            implements ConnectionFactory
    {
        private int numberOfFailure;
        int counter;

        public TestConnectionFactory(int numberOfFailure)
        {
            this.numberOfFailure = numberOfFailure;
        }

        @Override
        public Connection openConnection(JdbcIdentity identity)
                throws SQLException
        {
            counter++;
            if (counter <= numberOfFailure) {
                throw new SQLException("Failed to connect");
            }
            return mock(Connection.class);
        }

        @Override
        public void close()
                throws SQLException
        {
        }
    }
}
