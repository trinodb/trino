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
import io.trino.Session;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.EXECUTION_POLICY;
import static io.trino.SystemSessionProperties.FORCE_SPILLING_JOIN;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.MAX_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.MIN_HASH_PARTITION_COUNT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Runs joins whose tasks stop reading an exchange before all tasks of the stage feeding it exist.
 * The {@code slow} catalog returns one split per batch with a delay, so that its source stage creates
 * tasks one after another, and small output buffers make a producer block once a consumer stops reading.
 */
@Execution(SAME_THREAD)
final class TestExchangeConsumersFinishingEarly
        extends AbstractTestQueryFramework
{
    private static final long SPLIT_DELAY_MILLIS = 2_000;
    private static final String SINGLE_PART = "SELECT partkey FROM tpch.tiny.part WHERE name = 'goldenrod lavender spring chocolate lace'";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                .setSystemProperty(MIN_HASH_PARTITION_COUNT, "3")
                .setSystemProperty(MAX_HASH_PARTITION_COUNT, "3")
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(2)
                .addExtraProperty("sink.max-buffer-size", "4kB")
                .addExtraProperty("sink.max-broadcast-buffer-size", "4kB")
                .addExtraProperty("query.min-schedule-split-batch-size", "1")
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", Map.of("tpch.splits-per-node", "2"));
            queryRunner.createCatalog("tpch2", "tpch", Map.of("tpch.splits-per-node", "2"));
            queryRunner.installPlugin(new Plugin()
            {
                @Override
                public Iterable<ConnectorFactory> getConnectorFactories()
                {
                    return ImmutableList.of(new SlowSplitsTpchConnectorFactory());
                }
            });
            queryRunner.createCatalog("slow", "slow_tpch", Map.of("tpch.splits-per-node", "2"));
        }
        catch (Throwable e) {
            queryRunner.close();
            throw e;
        }
        return queryRunner;
    }

    @Test
    @Timeout(120)
    void testEmptyBuildOverPartitionedExchange()
    {
        assertQueryCompletes(
                getSession(),
                "SELECT count(*) FROM slow.sf1.partsupp ps JOIN (" + SINGLE_PART + ") p ON ps.partkey = p.partkey");
    }

    @Test
    @Timeout(120)
    void testEmptyBuildOverPartitionedExchangeWithSpillingJoin()
    {
        assertQueryCompletes(
                Session.builder(getSession())
                        .setSystemProperty(FORCE_SPILLING_JOIN, "true")
                        .build(),
                "SELECT count(*) FROM slow.sf1.partsupp ps JOIN (" + SINGLE_PART + ") p ON ps.partkey = p.partkey");
    }

    @Test
    @Timeout(120)
    void testEmptyBuildWhileTaskKeepsRunning()
    {
        assertQueryCompletes(
                Session.builder(getSession())
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                        .build(),
                """
                SELECT count(*)
                FROM (SELECT ps.partkey FROM slow.sf1.partsupp ps JOIN (%s) p ON ps.partkey = p.partkey) x
                RIGHT JOIN slow.sf1.partsupp big ON x.partkey = big.partkey
                """.formatted(SINGLE_PART));
    }

    @Test
    @Timeout(120)
    void testEmptyProbeOverPartitionedExchange()
    {
        assertQueryCompletes(
                Session.builder(getSession())
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                        .setSystemProperty(EXECUTION_POLICY, "all-at-once")
                        .build(),
                "SELECT count(*) FROM (" + SINGLE_PART + ") p JOIN slow.sf1.partsupp ps ON ps.partkey = p.partkey");
    }

    @Test
    @Timeout(120)
    void testEmptyProbeOverPartitionedExchangeWithDynamicFiltering()
    {
        assertQueryCompletes(
                Session.builder(getSession())
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                        .setSystemProperty(EXECUTION_POLICY, "all-at-once")
                        .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "true")
                        .build(),
                "SELECT count(*) FROM (" + SINGLE_PART + ") p JOIN slow.sf1.partsupp ps ON ps.partkey = p.partkey");
    }

    @Test
    @Timeout(120)
    void testEmptyProbeOverPartitionedExchangeWithSpillingJoin()
    {
        assertQueryCompletes(
                Session.builder(getSession())
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                        .setSystemProperty(EXECUTION_POLICY, "all-at-once")
                        .setSystemProperty(FORCE_SPILLING_JOIN, "true")
                        .build(),
                "SELECT count(*) FROM (" + SINGLE_PART + ") p JOIN slow.sf1.partsupp ps ON ps.partkey = p.partkey");
    }

    @Test
    @Timeout(120)
    void testEmptyProbeOverBroadcastExchange()
    {
        assertQueryCompletes(
                broadcastAllAtOnceSession(),
                "SELECT count(*) FROM (" + SINGLE_PART + ") p JOIN slow.sf1.partsupp ps ON ps.partkey = p.partkey");
    }

    @Test
    @Timeout(120)
    void testEmptyProbeOverBroadcastExchangeForCrossJoin()
    {
        assertQueryCompletes(
                broadcastAllAtOnceSession(),
                "SELECT count(*) FROM (" + SINGLE_PART + ") p CROSS JOIN slow.sf1.partsupp ps WHERE ps.availqty + p.partkey > 0");
    }

    @Test
    @Timeout(120)
    void testEmptyRoundRobinProbeOverBroadcastExchange()
    {
        assertQueryCompletes(
                broadcastAllAtOnceSession(),
                """
                SELECT count(*)
                FROM (%s UNION ALL SELECT partkey FROM tpch2.tiny.part WHERE name = 'blush thistle blue yellow saddle') p
                JOIN slow.sf1.partsupp ps ON ps.partkey = p.partkey
                """.formatted(SINGLE_PART));
    }

    private Session broadcastAllAtOnceSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(EXECUTION_POLICY, "all-at-once")
                .build();
    }

    private void assertQueryCompletes(Session session, String sql)
    {
        Object expected = computeActual(session, sql.replace("slow.", "tpch.")).getOnlyValue();
        assertThat(computeActual(session, sql).getOnlyValue()).isEqualTo(expected);
    }

    private static class SlowSplitsTpchConnectorFactory
            implements ConnectorFactory
    {
        private final TpchConnectorFactory delegate = new TpchConnectorFactory();

        @Override
        public String getName()
        {
            return "slow_tpch";
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            Connector connector = delegate.create(catalogName, config, context);
            ConnectorSplitManager splitManager = new ConnectorSplitManager()
            {
                @Override
                public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, Set<ColumnHandle> dynamicFilterColumns, Constraint constraint)
                {
                    return new SlowSplitSource(connector.getSplitManager().getSplits(transaction, session, table, dynamicFilterColumns, constraint));
                }
            };
            return new Connector()
            {
                @Override
                public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
                {
                    return connector.beginTransaction(isolationLevel, readOnly, autoCommit);
                }

                @Override
                public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
                {
                    return connector.getMetadata(session, transactionHandle);
                }

                @Override
                public ConnectorSplitManager getSplitManager()
                {
                    return splitManager;
                }

                @Override
                public ConnectorPageSourceProvider getPageSourceProvider()
                {
                    return connector.getPageSourceProvider();
                }

                @Override
                public ConnectorNodePartitioningProvider getNodePartitioningProvider()
                {
                    return connector.getNodePartitioningProvider();
                }

                @Override
                public void commit(ConnectorTransactionHandle transactionHandle)
                {
                    connector.commit(transactionHandle);
                }

                @Override
                public void rollback(ConnectorTransactionHandle transactionHandle)
                {
                    connector.rollback(transactionHandle);
                }

                @Override
                public void shutdown()
                {
                    connector.shutdown();
                }
            };
        }
    }

    private static class SlowSplitSource
            implements ConnectorSplitSource
    {
        private final ConnectorSplitSource delegate;
        private Deque<ConnectorSplit> splits;
        private boolean first = true;

        SlowSplitSource(ConnectorSplitSource delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public synchronized CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
        {
            if (splits == null) {
                splits = new ArrayDeque<>();
                while (!delegate.isFinished()) {
                    splits.addAll(delegate.getNextBatch(1000, dynamicFilterSnapshot).join());
                }
            }
            List<ConnectorSplit> batch = List.of(splits.removeFirst());
            if (first) {
                first = false;
                return CompletableFuture.completedFuture(batch);
            }
            return CompletableFuture.supplyAsync(() -> batch, delayedExecutor(SPLIT_DELAY_MILLIS, MILLISECONDS));
        }

        @Override
        public synchronized boolean isFinished()
        {
            return splits != null && splits.isEmpty();
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
