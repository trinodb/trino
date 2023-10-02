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

import com.google.inject.Guice;
import com.google.inject.Injector;
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
        Injector injector = Guice.createInjector(binder -> binder.bind(ConnectionFactory.class).annotatedWith(ForLazyConnectionFactory.class).toInstance(
                session -> {
                    throw new AssertionError("Expected no connection creation");
                }));

        try (LazyConnectionFactory lazyConnectionFactory = injector.getInstance(LazyConnectionFactory.class);
                Connection ignored = lazyConnectionFactory.openConnection(SESSION)) {
            // no-op
        }
    }

    @Test
    public void testConnectionCannotBeReusedAfterClose()
            throws Exception
    {
        BaseJdbcConfig config = new BaseJdbcConfig()
                .setConnectionUrl(format("jdbc:h2:mem:test%s;DB_CLOSE_DELAY=-1", System.nanoTime() + ThreadLocalRandom.current().nextLong()));

        Injector injector = Guice.createInjector(binder -> binder.bind(ConnectionFactory.class).annotatedWith(ForLazyConnectionFactory.class).toInstance(
            new DriverConnectionFactory(new Driver(), config, new EmptyCredentialProvider())));

        try (LazyConnectionFactory lazyConnectionFactory = injector.getInstance(LazyConnectionFactory.class)) {
            Connection connection = lazyConnectionFactory.openConnection(SESSION);
            connection.close();
            assertThatThrownBy(() -> connection.createStatement())
                    .hasMessage("Connection is already closed");
        }
    }
}
