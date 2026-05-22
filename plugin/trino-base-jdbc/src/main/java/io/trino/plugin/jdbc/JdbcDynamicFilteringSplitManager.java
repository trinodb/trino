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

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;

import java.util.Set;

import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static java.util.Objects.requireNonNull;

/**
 * Wraps {@link ConnectorSplitManager} to wait for dynamic filter collection before generating
 * splits, and to attach the resolved predicate to each split. Implementing this as a wrapper
 * allows JDBC connectors that don't use {@link JdbcSplitManager} to also benefit.
 */
public class JdbcDynamicFilteringSplitManager
        implements ConnectorSplitManager
{
    private final ConnectorSplitManager delegateSplitManager;

    @Inject
    public JdbcDynamicFilteringSplitManager(
            @ForJdbcDynamicFiltering ConnectorSplitManager delegateSplitManager)
    {
        this.delegateSplitManager = requireNonNull(delegateSplitManager, "delegateSplitManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        // JdbcProcedureHandle doesn't support any pushdown operation, so we rely on delegateSplitManager
        if (table instanceof JdbcProcedureHandle) {
            return delegateSplitManager.getSplits(transaction, session, table, dynamicFilterColumns, constraint);
        }

        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        // pushing DF through limit could reduce query performance
        boolean hasLimit = tableHandle.getLimit().isPresent();
        if (dynamicFilterColumns.isEmpty() || hasLimit || !dynamicFilteringEnabled(session)) {
            return delegateSplitManager.getSplits(transaction, session, table, dynamicFilterColumns, constraint);
        }

        return new DynamicFilteringJdbcSplitSource(delegateSplitManager, transaction, session, tableHandle, dynamicFilterColumns, constraint);
    }
}
