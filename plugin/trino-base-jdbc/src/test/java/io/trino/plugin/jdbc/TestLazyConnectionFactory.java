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
package io.trino.plugin.jdbc;

import io.trino.plugin.jdbc.credential.EmptyCredentialProvider;
import org.h2.Driver;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.concurrent.ThreadLocalRandom;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLazyConnectionFactory
{
    @Test
    public void testNoConnectionIsCreated()
            throws Exception
    {
        ConnectionFactory failingConnectionFactory = _ -> {
            throw new AssertionError("Expected no connection creation");
        };
        try (LazyConnectionFactory lazyConnectionFactory = new LazyConnectionFactory(failingConnectionFactory);
                Connection ignored = lazyConnectionFactory.openConnection(SESSION)) {
            // no-op
        }
    }

    @Test
    public void testConnectionCannotBeReusedAfterClose()
            throws Exception
    {
        String url = format("jdbc:h2:mem:test%s;DB_CLOSE_DELAY=-1", System.nanoTime() + ThreadLocalRandom.current().nextLong());
        try (DriverConnectionFactory driverConnectionFactory = DriverConnectionFactory.builder(new Driver(), url, new EmptyCredentialProvider()).build();
                LazyConnectionFactory lazyConnectionFactory = new LazyConnectionFactory(driverConnectionFactory)) {
            Connection connection = lazyConnectionFactory.openConnection(SESSION);
            connection.close();
            assertThatThrownBy(connection::createStatement)
                    .hasMessage("Connection is already closed");
        }
    }
}
