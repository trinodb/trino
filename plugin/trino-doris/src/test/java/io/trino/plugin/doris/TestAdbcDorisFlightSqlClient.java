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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

final class TestAdbcDorisFlightSqlClient
{
    @Test
    void testOpensNewExecutorForEachRequest()
    {
        TestingFlightSqlStreamOpenerFactory openerFactory = new TestingFlightSqlStreamOpenerFactory();
        AdbcDorisFlightSqlClient client = createClient(openerFactory);
        ConnectorSession session = session("query_reuse");

        DorisFlightSqlResult first = client.openStream(session, tableHandle(), split(), columns());
        first.close();

        DorisFlightSqlResult second = client.openStream(session, tableHandle(), split(), columns());
        second.close();

        assertThat(openerFactory.openers()).hasSize(2);
        assertThat(openerFactory.openers().stream().mapToInt(TestingFlightSqlStreamOpener::openCalls).sum()).isEqualTo(2);
        assertThat(openerFactory.openers().stream().mapToInt(TestingFlightSqlStreamOpener::closeCalls).sum()).isEqualTo(2);
    }

    @Test
    void testSessionLifecycleDoesNotAffectExecutorCreation()
    {
        TestingFlightSqlStreamOpenerFactory openerFactory = new TestingFlightSqlStreamOpenerFactory();
        AdbcDorisFlightSqlClient client = createClient(openerFactory);
        ConnectorSession session = session("query_no_lifecycle");

        DorisFlightSqlResult first = client.openStream(session, tableHandle(), split(), columns());
        first.close();

        DorisFlightSqlResult second = client.openStream(session, tableHandle(), split(), columns());
        second.close();

        assertThat(openerFactory.openers()).hasSize(2);
        assertThat(openerFactory.openers().stream()
                .mapToInt(TestingFlightSqlStreamOpener::closeCalls)
                .sum())
                .isEqualTo(2);
    }

    private static AdbcDorisFlightSqlClient createClient(TestingFlightSqlStreamOpenerFactory openerFactory)
    {
        DorisFlightSqlPortResolver portResolver = new DorisFlightSqlPortResolver()
        {
            @Override
            public int resolveFlightSqlPort()
            {
                return 9401;
            }
        };

        return new AdbcDorisFlightSqlClient(
                new DorisQueryBuilder(),
                portResolver,
                openerFactory,
                () -> List.of("fe1"));
    }

    private static DorisTableHandle tableHandle()
    {
        return new DorisTableHandle("sales", "orders");
    }

    private static DorisSplit split()
    {
        return new DorisSplit("sales", "orders", "", List.of(1L, 2L), Optional.empty());
    }

    private static List<DorisColumnHandle> columns()
    {
        return List.of(new DorisColumnHandle("order_id", BIGINT, 0));
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

    private static final class TestingFlightSqlStreamOpenerFactory
            implements AdbcDorisFlightSqlClient.FlightSqlStreamOpenerFactory
    {
        private final List<TestingFlightSqlStreamOpener> openers = new ArrayList<>();

        @Override
        public AdbcDorisFlightSqlClient.FlightSqlStreamOpener create(String feHost, int flightSqlPort)
        {
            TestingFlightSqlStreamOpener opener = new TestingFlightSqlStreamOpener(feHost, flightSqlPort);
            openers.add(opener);
            return opener;
        }

        public List<TestingFlightSqlStreamOpener> openers()
        {
            return openers;
        }
    }

    private static final class TestingFlightSqlStreamOpener
            implements AdbcDorisFlightSqlClient.FlightSqlStreamOpener
    {
        private final String host;
        private final int port;
        private final AtomicBoolean closed = new AtomicBoolean();
        private int openCalls;
        private int closeCalls;

        private TestingFlightSqlStreamOpener(String host, int port)
        {
            this.host = host;
            this.port = port;
        }

        @Override
        public DorisFlightSqlResult openStream(String sql)
        {
            assertThat(host).isEqualTo("fe1");
            assertThat(port).isEqualTo(9401);
            assertThat(sql).contains("FROM `sales`.`orders`");
            assertThat(closed.get()).isFalse();

            openCalls++;
            return new TestingDorisFlightSqlResult(this);
        }

        @Override
        public long getMemoryUsage()
        {
            return 128;
        }

        @Override
        public void close()
        {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            closeCalls++;
        }

        public int openCalls()
        {
            return openCalls;
        }

        public int closeCalls()
        {
            return closeCalls;
        }
    }

    private static final class TestingDorisFlightSqlResult
            implements DorisFlightSqlResult
    {
        private final TestingFlightSqlStreamOpener opener;
        private final AtomicBoolean closed = new AtomicBoolean();

        private TestingDorisFlightSqlResult(TestingFlightSqlStreamOpener opener)
        {
            this.opener = opener;
        }

        @Override
        public boolean loadNextBatch()
        {
            return false;
        }

        @Override
        public VectorSchemaRoot getVectorSchemaRoot()
        {
            return null;
        }

        @Override
        public long getMemoryUsage()
        {
            assertThat(closed.get()).isFalse();
            return opener.getMemoryUsage();
        }

        @Override
        public void close()
        {
            closed.set(true);
        }
    }
}
