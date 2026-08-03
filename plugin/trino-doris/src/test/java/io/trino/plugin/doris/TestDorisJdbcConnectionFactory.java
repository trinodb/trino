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
package io.trino.plugin.doris;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDorisJdbcConnectionFactory
{
    @Test
    void testOpensNewConnectionWithinSameQuery()
            throws Exception
    {
        TestingDriver driver = new TestingDriver();
        DorisJdbcConnectionFactory factory = new DorisJdbcConnectionFactory(driver, "jdbc:mysql://doris-fe:9030", new Properties());
        ConnectorSession session = session("query_reuse");

        Connection first = factory.openConnection(session);
        Connection second = factory.openConnection(session);

        first.close();
        second.close();

        assertThat(first).isNotSameAs(second);
        assertThat(driver.openedConnections()).isEqualTo(2);
        assertThat(driver.closedConnections()).isEqualTo(2);
    }

    @Test
    void testSessionConnectionClosesNormally()
            throws Exception
    {
        TestingDriver driver = new TestingDriver();
        DorisJdbcConnectionFactory factory = new DorisJdbcConnectionFactory(driver, "jdbc:mysql://doris-fe:9030", new Properties());
        ConnectorSession session = session("query_parallel");

        Connection connection = factory.openConnection(session);
        connection.close();

        assertThat(driver.openedConnections()).isEqualTo(1);
        assertThat(driver.closedConnections()).isEqualTo(1);
    }

    @Test
    void testConnectionMutationsDoNotAffectLifecycle()
            throws Exception
    {
        TestingDriver driver = new TestingDriver();
        DorisJdbcConnectionFactory factory = new DorisJdbcConnectionFactory(driver, "jdbc:mysql://doris-fe:9030", new Properties());
        ConnectorSession session = session("query_dirty");

        Connection connection = factory.openConnection(session);
        connection.setAutoCommit(false);
        connection.setReadOnly(false);
        connection.close();

        assertThat(driver.openedConnections()).isEqualTo(1);
        assertThat(driver.closedConnections()).isEqualTo(1);
    }

    @Test
    void testStandaloneConnectionCloses()
            throws Exception
    {
        TestingDriver driver = new TestingDriver();
        DorisJdbcConnectionFactory factory = new DorisJdbcConnectionFactory(driver, "jdbc:mysql://doris-fe:9030", new Properties());
        Connection connection = factory.openConnection();
        connection.close();

        assertThat(driver.openedConnections()).isEqualTo(1);
        assertThat(driver.closedConnections()).isEqualTo(1);
    }

    @Test
    void testSetReadOnlyFailureClosesConnection()
    {
        TestingDriver driver = new TestingDriver(true);
        DorisJdbcConnectionFactory factory = new DorisJdbcConnectionFactory(driver, "jdbc:mysql://doris-fe:9030", new Properties());

        assertThatThrownBy(factory::openConnection)
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("setReadOnly failed");

        assertThat(driver.openedConnections()).isEqualTo(1);
        assertThat(driver.closedConnections()).isEqualTo(1);
    }

    private static ConnectorSession session(String queryId)
    {
        return new ConnectorSession()
        {
            @Override
            public String getQueryId()
            {
                return queryId;
            }

            @Override
            public Optional<String> getSource()
            {
                return Optional.of("test");
            }

            @Override
            public ConnectorIdentity getIdentity()
            {
                return ConnectorIdentity.ofUser("test");
            }

            @Override
            public TimeZoneKey getTimeZoneKey()
            {
                return TimeZoneKey.UTC_KEY;
            }

            @Override
            public Locale getLocale()
            {
                return Locale.ENGLISH;
            }

            @Override
            public Instant getStart()
            {
                return Instant.EPOCH;
            }

            @Override
            public Optional<String> getTraceToken()
            {
                return Optional.empty();
            }

            @Override
            public <T> T getProperty(String name, Class<T> type)
            {
                throw new UnsupportedOperationException("No test session properties");
            }
        };
    }

    private static final class TestingDriver
            implements Driver
    {
        private final AtomicInteger openedConnections = new AtomicInteger();
        private final AtomicInteger closedConnections = new AtomicInteger();
        private final boolean failSetReadOnly;

        private TestingDriver()
        {
            this(false);
        }

        private TestingDriver(boolean failSetReadOnly)
        {
            this.failSetReadOnly = failSetReadOnly;
        }

        @Override
        public Connection connect(String url, Properties info)
        {
            openedConnections.incrementAndGet();
            return new TestingConnection(closedConnections::incrementAndGet, failSetReadOnly);
        }

        @Override
        public boolean acceptsURL(String url)
        {
            return true;
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
        {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion()
        {
            return 1;
        }

        @Override
        public int getMinorVersion()
        {
            return 0;
        }

        @Override
        public boolean jdbcCompliant()
        {
            return false;
        }

        @Override
        public Logger getParentLogger()
        {
            return Logger.getGlobal();
        }

        public int openedConnections()
        {
            return openedConnections.get();
        }

        public int closedConnections()
        {
            return closedConnections.get();
        }
    }

    private static final class TestingConnection
            extends ForwardingConnection
    {
        private final Runnable onClose;
        private final boolean failSetReadOnly;
        private boolean closed;

        private TestingConnection(Runnable onClose, boolean failSetReadOnly)
        {
            this.onClose = onClose;
            this.failSetReadOnly = failSetReadOnly;
        }

        @Override
        protected Connection delegate()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAutoCommit(boolean autoCommit) {}

        @Override
        public void setReadOnly(boolean readOnly)
                throws SQLException
        {
            if (failSetReadOnly) {
                throw new SQLException("setReadOnly failed");
            }
        }

        @Override
        public boolean isClosed()
        {
            return closed;
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            onClose.run();
        }
    }
}
