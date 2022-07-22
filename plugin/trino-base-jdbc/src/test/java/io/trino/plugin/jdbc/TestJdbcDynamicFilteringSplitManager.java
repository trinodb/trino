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
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.DYNAMIC_FILTERING_ENABLED;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJdbcDynamicFilteringSplitManager
{
    private static final ConnectorTransactionHandle TRANSACTION_HANDLE = TestingTransactionHandle.create();
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new JdbcDynamicFilteringSessionProperties(new JdbcDynamicFilteringConfig()).getSessionProperties())
            .setPropertyValues(ImmutableMap.of(
                    DYNAMIC_FILTERING_WAIT_TIMEOUT, "3s",
                    DYNAMIC_FILTERING_ENABLED, true))
            .build();
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
    };

    @Test
    public void testBlockingTimeout()
            throws Exception
    {
        JdbcDynamicFilteringSplitManager manager = new JdbcDynamicFilteringSplitManager(
                new TestingSplitManager(ImmutableList.of()),
                new DynamicFilteringStats());
        ConnectorSplitSource splitSource = manager.getSplits(
                TRANSACTION_HANDLE,
                SESSION,
                TABLE_HANDLE,
                BLOCKED_DYNAMIC_FILTER,
                alwaysTrue());

        // verify that getNextBatch() future completes after a timeout
        CompletableFuture<?> future = splitSource.getNextBatch(100);
        assertFalse(future.isDone());
        future.get(10, SECONDS);
        assertTrue(splitSource.isFinished());
        splitSource.close();
    }
}
