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
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingSplitManager;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.DYNAMIC_FILTERING_ENABLED;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJdbcDynamicFilteringSplitManager
{
    private record TestColumnHandle()
            implements ColumnHandle {}

    private static final JdbcColumnHandle JDBC_COLUMN_HANDLE = new JdbcColumnHandle("col", JDBC_BIGINT, BIGINT);
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

    @Test
    public void testRequestedDynamicFilterWaitTimeoutReported()
    {
        JdbcDynamicFilteringSplitManager manager = new JdbcDynamicFilteringSplitManager(
                new TestingSplitManager(ImmutableList.of()));
        Set<ColumnHandle> dynamicFilterColumns = ImmutableSet.of(new TestColumnHandle());
        ConnectorSplitSource splitSource = manager.getSplits(
                TRANSACTION_HANDLE,
                SESSION,
                TABLE_HANDLE,
                dynamicFilterColumns,
                alwaysTrue());
        assertThat(splitSource.getRequestedDynamicFilterWaitTimeoutMillis()).isEqualTo(3_000L);
        splitSource.close();
    }

    @Test
    public void testGetNextBatchUsesDynamicFilterPredicate()
            throws Exception
    {
        AtomicReference<ConnectorTableHandle> capturedTableHandle = new AtomicReference<>();
        JdbcDynamicFilteringSplitManager manager = new JdbcDynamicFilteringSplitManager(
                new ConnectorSplitManager()
                {
                    @Override
                    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, Set<ColumnHandle> dynamicFilterColumns, Constraint constraint)
                    {
                        capturedTableHandle.set(table);
                        return new FixedSplitSource(ImmutableList.of(new JdbcSplit(Optional.empty())));
                    }
                });
        Set<ColumnHandle> dynamicFilterColumns = ImmutableSet.of(new TestColumnHandle());
        ConnectorSplitSource splitSource = manager.getSplits(
                TRANSACTION_HANDLE,
                SESSION,
                TABLE_HANDLE,
                dynamicFilterColumns,
                alwaysTrue());

        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(JDBC_COLUMN_HANDLE, Domain.singleValue(BIGINT, 42L)));
        splitSource.getNextBatch(100, new DynamicFilterSnapshot(predicate, true)).get();

        assertThat(((JdbcTableHandle) capturedTableHandle.get()).getConstraint())
                .isEqualTo(predicate);
        splitSource.close();
    }

    @Test
    public void testGetNextBatchShortCircuitsOnNonePredicate()
            throws Exception
    {
        AtomicReference<ConnectorTableHandle> capturedTableHandle = new AtomicReference<>();
        JdbcDynamicFilteringSplitManager manager = new JdbcDynamicFilteringSplitManager(
                new ConnectorSplitManager()
                {
                    @Override
                    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, Set<ColumnHandle> dynamicFilterColumns, Constraint constraint)
                    {
                        capturedTableHandle.set(table);
                        return new FixedSplitSource(ImmutableList.of(new JdbcSplit(Optional.empty())));
                    }
                });
        Set<ColumnHandle> dynamicFilterColumns = ImmutableSet.of(new TestColumnHandle());
        ConnectorSplitSource splitSource = manager.getSplits(
                TRANSACTION_HANDLE,
                SESSION,
                TABLE_HANDLE,
                dynamicFilterColumns,
                alwaysTrue());

        List<ConnectorSplit> batch = splitSource.getNextBatch(100, new DynamicFilterSnapshot(TupleDomain.none(), true)).get();
        assertThat(batch).isEmpty();
        assertThat(capturedTableHandle.get()).isNull();
        splitSource.close();
    }
}
