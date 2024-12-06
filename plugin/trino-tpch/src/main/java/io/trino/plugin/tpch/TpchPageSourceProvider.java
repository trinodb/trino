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
package io.trino.plugin.tpch;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

public class TpchPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TpchRecordSetProvider tpchRecordSetProvider;
    private final int maxRowsPerPage;

    TpchPageSourceProvider(int maxRowsPerPage, DecimalTypeMapping decimalTypeMapping)
    {
        this.tpchRecordSetProvider = new TpchRecordSetProvider(decimalTypeMapping);
        this.maxRowsPerPage = maxRowsPerPage;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        return new LazyRecordPageSource(maxRowsPerPage, tpchRecordSetProvider.getRecordSet(transaction, session, split, table, columns));
    }

    @Override
    public TupleDomain<ColumnHandle> getUnenforcedPredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            TupleDomain<ColumnHandle> dynamicFilter)
    {
        // tpch connector doesn't support unenforced (effective) predicates
        return TupleDomain.all();
    }

    @Override
    public TupleDomain<ColumnHandle> prunePredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            TupleDomain<ColumnHandle> predicate)
    {
        // tpch connector doesn't support pruning of predicates
        return predicate;
    }
}
