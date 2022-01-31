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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingMetadata;
import io.trino.testing.TestingPageSinkProvider;
import io.trino.testing.TestingSplitManager;
import io.trino.testing.TestingTransactionHandle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestBeginQuery
        extends AbstractTestQueryFramework
{
    private TestMetadata metadata;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("test")
                .setSchema("default")
                .build();
        return DistributedQueryRunner.builder(session).build();
    }

    @BeforeClass
    public void setUp()
    {
        metadata = new TestMetadata();
        getQueryRunner().installPlugin(new TestPlugin(metadata));
        getQueryRunner().installPlugin(new TpchPlugin());
        getQueryRunner().createCatalog("test", "test", ImmutableMap.of());
        getQueryRunner().createCatalog("tpch", "tpch", ImmutableMap.of());
    }

    @AfterMethod(alwaysRun = true)
    public void afterMethod()
    {
        if (metadata != null) {
            metadata.clear();
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (metadata != null) {
            metadata.clear();
            metadata = null;
        }
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
    }

    @Test
    public void testCreateTableAsSelectSameConnector()
    {
        assertBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("CREATE TABLE nation_copy AS SELECT * FROM nation");
    }

    @Test
    public void testInsert()
    {
        assertBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("INSERT INTO nation SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("INSERT INTO nation VALUES (12345, 'name', 54321, 'comment')");
    }

    @Test
    public void testInsertSelectSameConnector()
    {
        assertBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("INSERT INTO nation SELECT * FROM nation");
    }

    @Test
    public void testSelect()
    {
        assertBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("SELECT * FROM nation");
    }

    private void assertBeginQuery(String query)
    {
        metadata.resetCounters();
        computeActual(query);
        assertEquals(metadata.begin.get(), 1);
        assertEquals(metadata.end.get(), 1);
        metadata.resetCounters();
    }

    private static class TestPlugin
            implements Plugin
    {
        private final TestMetadata metadata;

        private TestPlugin(TestMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new ConnectorFactory()
            {
                @Override
                public String getName()
                {
                    return "test";
                }

                @Override
                public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                {
                    return new TestConnector(metadata);
                }
            });
        }
    }

    private static class TestConnector
            implements Connector
    {
        private final ConnectorMetadata metadata;

        private TestConnector(ConnectorMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
        {
            return TestingTransactionHandle.create();
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
        {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return new TestingSplitManager(ImmutableList.of());
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider()
        {
            return new ConnectorPageSourceProvider()
            {
                @Override
                public ConnectorPageSource createPageSource(
                        ConnectorTransactionHandle transaction,
                        ConnectorSession session,
                        ConnectorSplit split,
                        ConnectorTableHandle table,
                        List<ColumnHandle> columns,
                        DynamicFilter dynamicFilter)
                {
                    return new EmptyPageSource();
                }
            };
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider()
        {
            return new TestingPageSinkProvider();
        }
    }

    private static class TestMetadata
            extends TestingMetadata
    {
        private final AtomicInteger begin = new AtomicInteger();
        private final AtomicInteger end = new AtomicInteger();

        @Override
        public void beginQuery(ConnectorSession session)
        {
            begin.incrementAndGet();
        }

        @Override
        public void cleanupQuery(ConnectorSession session)
        {
            end.incrementAndGet();
        }

        public void resetCounters()
        {
            begin.set(0);
            end.set(0);
        }
    }
}
