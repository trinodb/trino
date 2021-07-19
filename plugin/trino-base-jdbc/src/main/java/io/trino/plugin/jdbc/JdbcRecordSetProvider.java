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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.DynamicFilteringJdbcSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.jdbc.DynamicFilteringJdbcSessionProperties.isEnableDynamicFiltering;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JdbcRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final JdbcClient jdbcClient;
    private final ExecutorService executor;

    @Inject
    public JdbcRecordSetProvider(JdbcClient jdbcClient, @ForRecordCursor ExecutorService executor)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        if (isEnableDynamicFiltering(session)) {
            long timeoutMillis = getDynamicFilteringWaitTimeout(session).toMillis();

            // no time for further narrow down or done already
            if (timeoutMillis == 0 || !dynamicFilter.isAwaitable()) {
                JdbcTableHandle handle = ((JdbcTableHandle) table).withConstraint(dynamicFilter.getCurrentPredicate());
                return getRecordSet(transaction, session, split, handle, columns);
            }

            try {
                dynamicFilter.isBlocked().get(timeoutMillis, MILLISECONDS);
                JdbcTableHandle handle = ((JdbcTableHandle) table).withConstraint(dynamicFilter.getCurrentPredicate());
                return getRecordSet(transaction, session, split, handle, columns);
            }
            catch (Exception e) {
                return getRecordSet(transaction, session, split, table, columns);
            }
        }

        return getRecordSet(transaction, session, split, table, columns);
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        JdbcSplit jdbcSplit = (JdbcSplit) split;
        JdbcTableHandle jdbcTable = (JdbcTableHandle) table;

        // In the current API, the columns (and order) needed by the engine are provided via an argument to this method. Make sure we can
        // satisfy the requirements using columns which were recorded in the table handle.
        // If no columns are recorded, it means that applyProjection never got called (e.g., in the case all columns are being used) and all
        // table columns should be returned. TODO: this is something that should be addressed once the getRecordSet API is revamped
        jdbcTable.getColumns()
                .ifPresent(tableColumns -> verify(ImmutableSet.copyOf(tableColumns).containsAll(columns)));

        ImmutableList.Builder<JdbcColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((JdbcColumnHandle) handle);
        }

        return new JdbcRecordSet(jdbcClient, executor, session, jdbcSplit, jdbcTable, handles.build());
    }
}
