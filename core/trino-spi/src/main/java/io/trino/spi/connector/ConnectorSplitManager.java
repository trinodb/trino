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
package io.trino.spi.connector;

import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.Set;

public interface ConnectorSplitManager
{
    /**
     * Returns splits for a table scan.
     * <p>
     * {@code dynamicFilterColumns} is the static set of columns the dynamic filter will cover.
     * Connectors may use this to make planning-time decisions (e.g. which column statistics to
     * read). The per-batch resolved predicate arrives separately via
     * {@link ConnectorSplitSource#getNextBatch(int, DynamicFilterSnapshot)}.
     */
    ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint);

    default ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function)
    {
        throw new UnsupportedOperationException();
    }
}
