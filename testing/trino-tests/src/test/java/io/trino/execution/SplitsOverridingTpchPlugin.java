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
import io.trino.plugin.tpch.TpchConnectorFactory;
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
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.testing.QueryRunner;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * TPC-H connectors with changed splits. The {@code slow} catalog returns one split per batch, each but the first
 * after a delay, so that its source stage creates tasks one after another. The {@code no_splits} catalog returns no splits.
 */
public final class SplitsOverridingTpchPlugin
        implements Plugin
{
    private static final long SPLIT_DELAY_MILLIS = 2_000;

    public static void createCatalogs(QueryRunner queryRunner)
    {
        queryRunner.installPlugin(new SplitsOverridingTpchPlugin());
        queryRunner.createCatalog("slow", "slow_tpch", Map.of("tpch.splits-per-node", "2"));
        queryRunner.createCatalog("no_splits", "no_splits_tpch", Map.of());
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(
                new SplitsOverridingTpchConnectorFactory("slow_tpch", SlowSplitSource::new),
                new SplitsOverridingTpchConnectorFactory("no_splits_tpch", _ -> new FixedSplitSource(ImmutableList.of())));
    }

    private static class SplitsOverridingTpchConnectorFactory
            implements ConnectorFactory
    {
        private final TpchConnectorFactory delegate = new TpchConnectorFactory();
        private final String name;
        private final UnaryOperator<ConnectorSplitSource> splitSourceOverride;

        SplitsOverridingTpchConnectorFactory(String name, UnaryOperator<ConnectorSplitSource> splitSourceOverride)
        {
            this.name = requireNonNull(name, "name is null");
            this.splitSourceOverride = requireNonNull(splitSourceOverride, "splitSourceOverride is null");
        }

        @Override
        public String getName()
        {
            return name;
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
                    return splitSourceOverride.apply(connector.getSplitManager().getSplits(transaction, session, table, dynamicFilterColumns, constraint));
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
