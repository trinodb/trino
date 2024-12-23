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
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.ConnectorCacheMetadata;
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
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingMetadata;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.testing.TestingPageSinkProvider;
import io.trino.testing.TestingTransactionHandle;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingSplit.createRemoteSplit;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCacheDynamicFiltering
        extends AbstractTestQueryFramework
{
    private volatile Consumer<TupleDomain<ColumnHandle>> expectedCoordinatorDynamicFilterAssertion;
    private volatile Predicate<TupleDomain<ColumnHandle>> expectedTableScanDynamicFilter;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("test")
                .setSchema("default")
                .build();
        return DistributedQueryRunner.builder(session)
                .setExtraProperties(ImmutableMap.of(
                        "cache.enabled", "true",
                        "query.schedule-split-batch-size", "1"))
                .build();
    }

    @BeforeAll
    public void setup()
    {
        getQueryRunner().installPlugin(new TestingPlugin());
        getQueryRunner().installPlugin(new TpchPlugin());
        getQueryRunner().createCatalog("test", "test", ImmutableMap.of());
        getQueryRunner().createCatalog("tpch", "tpch", ImmutableMap.of());
        computeActual("CREATE TABLE orders AS SELECT * FROM tpch.tiny.orders");
    }

    @Test
    public void testCacheDynamicFiltering()
    {
        @Language("SQL") String query = """
                select count(orderkey) from orders o join (select * from (values 0, 1) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from orders o join (select * from (values 0, 2) t(custkey)) t on o.custkey = t.custkey
                """;
        TupleDomain<ColumnHandle> firstScanDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("custkey", OptionalInt.of(1), Optional.of(BIGINT)), Domain.create(ValueSet.of(BIGINT, 0L, 1L), false)));
        TupleDomain<ColumnHandle> secondScanDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("custkey", OptionalInt.of(1), Optional.of(BIGINT)), Domain.create(ValueSet.of(BIGINT, 0L, 2L), false)));
        // DF on worker nodes should eventually be union of DFs from first and second orders table scan
        expectedTableScanDynamicFilter = tuple -> tuple.equals(TupleDomain.columnWiseUnion(firstScanDomain, secondScanDomain));
        // Coordinator should only use original DF for split enumeration
        expectedCoordinatorDynamicFilterAssertion = tuple -> assertThat(tuple).isIn(firstScanDomain, secondScanDomain);
        computeActual(query);
    }

    private class TestingPlugin
            implements Plugin
    {
        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new ConnectorFactory()
            {
                private final ConnectorCacheMetadata metadata = new ConnectorCacheMetadata()
                {
                    @Override
                    public Optional<CacheTableId> getCacheTableId(ConnectorTableHandle tableHandle)
                    {
                        return Optional.of(new CacheTableId(((TestingTableHandle) tableHandle).getTableName().getTableName()));
                    }

                    @Override
                    public Optional<CacheColumnId> getCacheColumnId(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
                    {
                        return Optional.of(new CacheColumnId(((TestingColumnHandle) columnHandle).getName()));
                    }

                    @Override
                    public ConnectorTableHandle getCanonicalTableHandle(ConnectorTableHandle handle)
                    {
                        return handle;
                    }
                };

                @Override
                public String getName()
                {
                    return "test";
                }

                @Override
                public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                {
                    return new TestConnector(new TestingMetadata(), metadata);
                }
            });
        }
    }

    private class TestConnector
            implements Connector
    {
        private final ConnectorMetadata metadata;
        private final ConnectorCacheMetadata cacheMetadata;
        private final AtomicLong splitCount = new AtomicLong();
        private volatile boolean finished;

        private TestConnector(ConnectorMetadata metadata, ConnectorCacheMetadata cacheMetadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.cacheMetadata = requireNonNull(cacheMetadata, "cacheMetadata is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
        {
            return TestingTransactionHandle.create();
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
        {
            return metadata;
        }

        @Override
        public ConnectorCacheMetadata getCacheMetadata()
        {
            return cacheMetadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return new ConnectorSplitManager()
            {
                @Override
                public ConnectorSplitSource getSplits(
                        ConnectorTransactionHandle transaction,
                        ConnectorSession session,
                        ConnectorTableHandle table,
                        DynamicFilter dynamicFilter,
                        Constraint constraint)
                {
                    return new ConnectorSplitSource()
                    {
                        @Override
                        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
                        {
                            CompletableFuture<?> blocked = dynamicFilter.isBlocked();

                            if (blocked.isDone()) {
                                // prevent active looping
                                try {
                                    Thread.sleep(100);
                                }
                                catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                return completedFuture(new ConnectorSplitBatch(ImmutableList.of(createRemoteSplit()), isFinished()));
                            }

                            return blocked.thenApply(ignored -> {
                                // yield until dynamic filter is fully loaded
                                return new ConnectorSplitBatch(ImmutableList.of(), false);
                            });
                        }

                        @Override
                        public void close()
                        {
                        }

                        @Override
                        public boolean isFinished()
                        {
                            if (!finished) {
                                return false;
                            }

                            expectedCoordinatorDynamicFilterAssertion.accept(dynamicFilter.getCurrentPredicate());
                            return true;
                        }
                    };
                }

                @Override
                public Optional<CacheSplitId> getCacheSplitId(ConnectorSplit split)
                {
                    return Optional.of(new CacheSplitId(Long.toString(splitCount.incrementAndGet())));
                }
            };
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
                    return new EmptyPageSource()
                    {
                        @Override
                        public boolean isFinished()
                        {
                            // cache DF on worker should not block
                            assertThat(dynamicFilter.isBlocked()).isDone();
                            if (expectedTableScanDynamicFilter.test(dynamicFilter.getCurrentPredicate())) {
                                finished = true;
                            }

                            return true;
                        }
                    };
                }

                @Override
                public TupleDomain<ColumnHandle> getUnenforcedPredicate(
                        ConnectorSession session,
                        ConnectorSplit split,
                        ConnectorTableHandle table,
                        TupleDomain<ColumnHandle> dynamicFilter)
                {
                    return dynamicFilter;
                }

                @Override
                public TupleDomain<ColumnHandle> prunePredicate(
                        ConnectorSession session,
                        ConnectorSplit split,
                        ConnectorTableHandle table,
                        TupleDomain<ColumnHandle> predicate)
                {
                    return predicate;
                }
            };
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider()
        {
            return new TestingPageSinkProvider();
        }
    }
}
