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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingSplitManager;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.DYNAMIC_FILTERING_ENABLED;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestJdbcDynamicFilteringSplitManager
{
    private static final ConnectorTransactionHandle TRANSACTION_HANDLE = TestingTransactionHandle.create();
    private static final JdbcTableHandle TABLE_HANDLE = new JdbcTableHandle(
            new SchemaTableName("schema", "table"),
            new RemoteTableName(Optional.empty(), Optional.empty(), "table"),
            Optional.empty());
    private static final DynamicFilter BLOCKED_DYNAMIC_FILTER = new DynamicFilter()
    {
        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return ImmutableSet.of();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return unmodifiableFuture(new CompletableFuture<>());
        }

        @Override
        public boolean isComplete()
        {
            return false;
        }

        @Override
        public boolean isAwaitable()
        {
            return true;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return TupleDomain.all();
        }

        @Override
        public OptionalLong getPreferredDynamicFilterTimeout()
        {
            return OptionalLong.of(0L);
        }
    };

    @Test
    public void testBlockingTimeout()
            throws Exception
    {
        TestingConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new JdbcDynamicFilteringSessionProperties(new JdbcDynamicFilteringConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT, "3s",
                        DYNAMIC_FILTERING_ENABLED, true))
                .build();
        ConnectorSplitSource splitSource = getConnectorSplitSource(session, BLOCKED_DYNAMIC_FILTER);
        // verify that getNextBatch() future completes after a timeout
        CompletableFuture<?> future = splitSource.getNextBatch(100);
        assertThat(future.isDone()).isFalse();
        future.get(10, SECONDS);
        assertThat(splitSource.isFinished()).isTrue();
        splitSource.close();
    }

    @Test
    public void testMinDynamicFilterBlockingTimeout()
            throws Exception
    {
        TestingConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new JdbcDynamicFilteringSessionProperties(new JdbcDynamicFilteringConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT, "0s",
                        DYNAMIC_FILTERING_ENABLED, true))
                .build();
        CompletableFuture<Void> dynamicFilterFuture = new CompletableFuture<>();
        DynamicFilter dynamicFilter = new DynamicFilter()
        {
            final List<CompletableFuture<Void>> lazyDynamicFilterFutures = ImmutableList.of(
                    dynamicFilterFuture,
                    new CompletableFuture<>());

            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return ImmutableSet.of();
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return unmodifiableFuture(CompletableFuture.anyOf(getUndoneFutures().toArray(new CompletableFuture[0])));
            }

            @Override
            public boolean isComplete()
            {
                return getUndoneFutures().isEmpty();
            }

            @Override
            public boolean isAwaitable()
            {
                return !isComplete();
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return TupleDomain.all();
            }

            @Override
            public OptionalLong getPreferredDynamicFilterTimeout()
            {
                return getUndoneFutures().isEmpty() ? OptionalLong.of(0L) : OptionalLong.of(3000L);
            }

            private List<CompletableFuture<Void>> getUndoneFutures()
            {
                return lazyDynamicFilterFutures.stream()
                        .filter(future -> !future.isDone())
                        .collect(toImmutableList());
            }
        };
        ConnectorSplitSource splitSource = getConnectorSplitSource(session, dynamicFilter);

        // verify that getNextBatch() future completes after a min dynamic filter timeout
        CompletableFuture<?> splitSourceNextBatchFuture = splitSource.getNextBatch(100);
        assertFalse(splitSourceNextBatchFuture.isDone());
        // first narrow down of DF
        dynamicFilterFuture.complete(null);
        assertTrue(splitSourceNextBatchFuture.isDone());
        // whole DF is not completed, still min dynamic filter timeout remains
        assertFalse(splitSource.isFinished());
        splitSourceNextBatchFuture = splitSource.getNextBatch(100);
        assertFalse(splitSourceNextBatchFuture.isDone());
        assertFalse(splitSource.isFinished());
        // await preferred timeout ~ 3s
        splitSourceNextBatchFuture.get(20, SECONDS);
        assertTrue(splitSourceNextBatchFuture.isDone());
        // preferred timeout passed but dynamic filter is still not done
        assertTrue(dynamicFilter.isAwaitable());
        // split source is completed
        assertTrue(splitSource.isFinished());
        splitSource.close();
    }

    private static ConnectorSplitSource getConnectorSplitSource(ConnectorSession session, DynamicFilter blockedDynamicFilter)
    {
        JdbcDynamicFilteringSplitManager manager = new JdbcDynamicFilteringSplitManager(
                new TestingSplitManager(ImmutableList.of()),
                new DynamicFilteringStats());

        return manager.getSplits(
                TRANSACTION_HANDLE,
                session,
                TABLE_HANDLE,
                blockedDynamicFilter,
                alwaysTrue());
    }
}
